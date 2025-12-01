"""
消息处理器 - 使用过滤器链模式（基于 TelegramForwarder 的高性能设计）
只保留关键词监听和转发功能，去除冗余功能
"""
import asyncio
import logging
from core.filter_chain import FilterChain, MessageContext
from core.message_filters import KeywordFilter, SenderFilter, TemplateSenderFilter
from core.filters import match_keywords_normalized
from services import settings_service

logger = logging.getLogger(__name__)

# 每个账号的并发控制（防止封号）
_ACCOUNT_SEMAPHORES: dict[int, tuple[asyncio.Semaphore, int]] = {}


def _get_semaphore(account_id: int) -> asyncio.Semaphore:
    """获取账号的信号量，用于控制并发数（全速运行：最大化并发）"""
    # 全速运行：默认并发数设为100，充分利用CPU和内存
    value = max(100, settings_service.get_concurrency(account_id) or 100)
    sem, current = _ACCOUNT_SEMAPHORES.get(account_id, (None, 0))
    if sem is None or current != value:
        sem = asyncio.Semaphore(value)
        _ACCOUNT_SEMAPHORES[account_id] = (sem, value)
    return sem


async def on_new_message(event, account: dict, bot_client, control_bot_id=None):
    """处理新消息：使用过滤器链模式（高性能，基于 TelegramForwarder 设计）
    
    Args:
        event: Telethon 消息事件
        account: 账号信息字典
        bot_client: 控制机器人客户端
        control_bot_id: 控制机器人的 ID（用于过滤自己的消息）
    """
    account_id = account['id']
    try:
        chat_id = getattr(event, 'chat_id', None)
        msg_id = getattr(event.message, 'id', None)
        msg_text = getattr(event.message, 'message', '') or getattr(event.message, 'text', '') or ''
        print(f"[on_new_message] 账号 #{account_id} 开始处理: Chat ID={chat_id}, Msg ID={msg_id}, 文本长度={len(msg_text)}")
        
        # 放宽过滤条件：只跳过私聊和自己发送的消息，其他所有消息都处理
        # 这样可以监听所有群组、超级群组、频道等
        if event.is_private:
            print(f"[on_new_message] 账号 #{account_id} 跳过私聊消息: Chat ID={chat_id}")
            return
        
        if event.message.out:
            print(f"[on_new_message] 账号 #{account_id} 跳过自己发送的消息: Chat ID={chat_id}")
            return
        
        # 获取群组类型信息（用于日志）
        is_group = event.is_group
        is_megagroup = False
        is_broadcast = False
        try:
            if hasattr(event, 'chat') and event.chat:
                is_megagroup = getattr(event.chat, 'megagroup', False)
                is_broadcast = getattr(event.chat, 'broadcast', False)
        except:
            pass
        
        # 记录所有处理的消息（特别是目标群组）
        if chat_id == -1002964498071:
            print(f"[🔍 诊断] 账号 #{account_id} 目标群组消息进入处理: Chat ID={chat_id}")
            print(f"[🔍 诊断] 群组类型: is_group={is_group}, is_megagroup={is_megagroup}, is_broadcast={is_broadcast}")
        
        # 所有非私聊、非自己发送的消息都会继续处理
        
        role = settings_service.get_account_role(account_id) or 'both'
        print(f"[on_new_message] 账号 #{account_id} 角色: {role}")
        
        # =================================================================
        # 1) 关键词监听（仅当角色包含 listen）- 使用过滤器链
        # =================================================================
        if role in ('listen', 'both'):
            print(f"[on_new_message] 账号 #{account_id} 是监听账号，创建过滤器链")
            # 创建消息上下文
            context = MessageContext(
                client=event.client,
                event=event,
                account=account,
                bot_client=bot_client,
                control_bot_id=control_bot_id
            )
            
            # 创建过滤器链
            filter_chain = FilterChain()
            
            # 添加过滤器（按顺序执行）
            filter_chain.add_filter(KeywordFilter())      # 1. 关键词匹配
            filter_chain.add_filter(SenderFilter())       # 2. 发送提醒
            filter_chain.add_filter(TemplateSenderFilter())  # 3. 发送模板消息（如果启用）
            
            # 执行过滤器链（完全异步，不阻塞）
            # 立即创建任务，不等待完成，真正并发
            print(f"[on_new_message] 账号 #{account_id} 启动过滤器链处理任务")
            asyncio.create_task(filter_chain.process(context))
        else:
            print(f"[on_new_message] 账号 #{account_id} 不是监听账号，跳过关键词监听")

        # =================================================================
        # 2) 按钮点击（仅当角色包含 click）- 保持原有逻辑
        # =================================================================
        if role in ('click', 'both'):
            buttons = getattr(event.message, 'buttons', None)
            if buttons:
                keywords = settings_service.get_account_keywords(account['id'], kind='click') or []
                if keywords:
                    # 遍历按钮，查找匹配（使用规范化匹配，处理emoji和零宽字符）
                    for i, row in enumerate(buttons):
                        for j, btn in enumerate(row):
                            btn_text = getattr(btn, 'text', None) or ''
                            # 使用规范化匹配，可以处理包含emoji和零宽字符的按钮文本
                            matched_keyword = match_keywords_normalized(account['id'], btn_text, kind='click')
                            if matched_keyword:
                                logger.info(f"[点击] ✅ 账号 #{account['id']} 匹配按钮: '{btn_text}' (关键词: {matched_keyword})")
                                
                                # 定义点击任务（全速运行：立即点击，无延迟）
                                async def _click_button(row_idx, col_idx, b_text):
                                    try:
                                        # 全速运行：移除所有延迟，立即点击
                                        # 尝试点击（使用信号量控制并发）
                                        sem = _get_semaphore(account['id'])
                                        async with sem:
                                            logger.info(f"[点击] 账号 #{account['id']} 立即点击按钮 [{row_idx},{col_idx}] '{b_text}'")
                                            await event.click(row_idx, col_idx)
                                            logger.info(f"[点击] ✅ 账号 #{account['id']} 点击成功（按钮：{b_text}）")
                                    except Exception as e:
                                        error_str = str(e)
                                        error_type = type(e).__name__
                                        logger.error(f"[点击] ❌ 账号 #{account['id']} 点击失败：{error_type}: {error_str}")
                                        import traceback
                                        traceback.print_exc()

                                # 启动后台任务
                                asyncio.create_task(_click_button(i, j, btn_text))
                                return  # 匹配到一个按钮后通常停止匹配后续按钮

    except (GeneratorExit, RuntimeError) as e:
        # 忽略常见的异步关闭错误
        if 'GeneratorExit' in str(type(e).__name__) or 'coroutine ignored' in str(e):
            return
        logger.warning(f"[监听] ⚠️ 账号 #{account.get('id', '?')} RuntimeError: {str(e)}")
    except Exception as e:
        logger.error(f"[监听] ❌ 账号 #{account.get('id', '?')} 错误: {str(e)}")
import asyncio
import random
from datetime import datetime
from core.filters import match_keywords, match_keywords_normalized
from services.alerting import send_alert
from services import settings_service

# 每个账号的并发控制（防止封号）
_ACCOUNT_SEMAPHORES: dict[int, tuple[asyncio.Semaphore, int]] = {}


def _get_semaphore(account_id: int) -> asyncio.Semaphore:
    """获取账号的信号量，用于控制并发数"""
    value = max(1, settings_service.get_concurrency(account_id))
    sem, current = _ACCOUNT_SEMAPHORES.get(account_id, (None, 0))
    if sem is None or current != value:
        sem = asyncio.Semaphore(value)
        _ACCOUNT_SEMAPHORES[account_id] = (sem, value)
    return sem


async def on_new_message(event, account: dict, bot_client, control_bot_id=None):
    """处理新消息：监听关键词和点击按钮（支持多账号并发）
    
    Args:
        event: Telethon 消息事件
        account: 账号信息字典
        bot_client: 控制机器人客户端
        control_bot_id: 控制机器人的 ID（用于过滤自己的消息）
    """
    try:
        # 快速过滤：跳过私聊、非群组、自己发送的消息
        if event.is_private or not event.is_group or event.message.out:
            return
        
        # 获取消息文本
        text = event.message.message or ''
        if not text:
            text = getattr(event.message, 'raw_text', '') or ''
            if not text:
                text = str(event.message.text) if hasattr(event.message, 'text') else ''
        
        role = settings_service.get_account_role(account['id']) or 'both'

        # =================================================================
        # 1) 关键词监听（仅当角色包含 listen）
        # =================================================================
        if role in ('listen', 'both'):
            keywords = settings_service.get_account_keywords(account['id'], kind='listen') or []
            matched = None
            if keywords:
                matched = match_keywords(account['id'], text, kind='listen')
            
            if matched:
                # 获取消息ID用于日志
                msg_id = getattr(event.message, 'id', None)
                chat_id = getattr(event, 'chat_id', None)
                print(f"[监听] ✅ 账号 #{account['id']} 匹配关键词: '{matched}' (消息ID: {msg_id}, Chat ID: {chat_id})")
                
                # 检查是否需要过滤机器人消息
                should_alert = True
                target = settings_service.get_target_chat()
                print(f"[监听] 转发目标: {target if target else '未设置'}")
                
                if target and target.strip() and bot_client:
                    try:
                        sender = await event.get_sender()
                        sender_id = getattr(sender, 'id', None)
                        is_bot = getattr(sender, 'bot', False)
                        
                        # 只有当消息来自控制机器人本身时才跳过
                        # 其他机器人的消息正常处理
                        if is_bot and control_bot_id and sender_id == control_bot_id:
                            print(f"[监听] ⚠️ 消息来自控制机器人本身（ID: {sender_id}），跳过发送提醒")
                            should_alert = False
                        else:
                            if is_bot:
                                print(f"[监听] 消息来自其他机器人（ID: {sender_id}），允许发送提醒")
                            else:
                                print(f"[监听] 消息来自用户（ID: {sender_id}），允许发送提醒")
                    except Exception as e:
                        print(f"[监听] ⚠️ 获取发送者失败: {str(e)}，默认允许发送")
                        pass # 获取发送者失败，默认不跳过
                else:
                    if not target or not target.strip():
                        print(f"[监听] ⚠️ 转发目标未设置，跳过发送提醒")
                    if not bot_client:
                        print(f"[监听] ⚠️ bot_client 为空，跳过发送提醒")
                    should_alert = False
                
                if should_alert:
                    # 优化：立即发送提醒，不等待（使用 create_task 异步执行）
                    # 这样不会阻塞消息处理，极大提升监听速度
                    async def _send_alert_task():
                        try:
                            print(f"[监听] 准备发送提醒...")
                            await send_alert(bot_client, account, event, matched)
                            print(f"[监听] ✅ 提醒发送成功")
                        except Exception as e:
                            print(f"[监听] ❌ 发送提醒失败: {str(e)}")
                            import traceback
                            traceback.print_exc()
                    
                    # 立即创建任务，不等待完成
                    asyncio.create_task(_send_alert_task())
                else:
                    print(f"[监听] ⚠️ 跳过发送提醒（should_alert=False）")
                    
                # 自动发送模板消息（全速运行：立即发送，无延迟）
                if settings_service.get_start_sending(account['id']):
                    tpl = settings_service.get_template_message(account['id'])
                    if tpl:
                        async def _send_template():
                            try:
                                # 全速运行：移除所有延迟，立即发送
                                sem = _get_semaphore(account['id'])
                                async with sem:
                                    await event.client.send_message(event.chat_id, tpl)
                            except Exception:
                                pass
                        
                        # 创建后台任务执行发送（立即调度，不等待）
                        asyncio.create_task(_send_template())

        # =================================================================
        # 2) 按钮点击（仅当角色包含 click）
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
                                print(f"[点击] ✅ 账号 #{account['id']} 匹配按钮: '{btn_text}' (关键词: {matched_keyword})")
                                
                                # 定义点击任务（全速运行：立即点击，无延迟）
                                async def _click_button(row_idx, col_idx, b_text):
                                    try:
                                        # 全速运行：移除所有延迟，立即点击
                                        # 尝试点击（使用信号量控制并发）
                                        sem = _get_semaphore(account['id'])
                                        async with sem:
                                            print(f"[点击] 账号 #{account['id']} 立即点击按钮 [{row_idx},{col_idx}] '{b_text}'")
                                            await event.click(row_idx, col_idx)
                                            print(f"[点击] ✅ 账号 #{account['id']} 点击成功（按钮：{b_text}）")
                                    except Exception as e:
                                        error_str = str(e)
                                        error_type = type(e).__name__
                                        print(f"[点击] ❌ 账号 #{account['id']} 点击失败：{error_type}: {error_str}")
                                        import traceback
                                        traceback.print_exc()

                                # 启动后台任务
                                asyncio.create_task(_click_button(i, j, btn_text))
                                return # 匹配到一个按钮后通常停止匹配后续按钮

    except (GeneratorExit, RuntimeError) as e:
        # 忽略常见的异步关闭错误
        if 'GeneratorExit' in str(type(e).__name__) or 'coroutine ignored' in str(e):
            return
        print(f"[监听] ⚠️ 账号 #{account.get('id', '?')} RuntimeError: {str(e)}")
    except Exception as e:
        print(f"[监听] ❌ 账号 #{account.get('id', '?')} 错误: {str(e)}")
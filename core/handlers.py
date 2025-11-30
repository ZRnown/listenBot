import asyncio
import random
from datetime import datetime
from core.filters import match_keywords
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
                    print(f"[监听] 准备发送提醒...")
                    try:
                        await send_alert(bot_client, account, event, matched)
                        print(f"[监听] ✅ 提醒发送成功")
                    except Exception as e:
                        print(f"[监听] ❌ 发送提醒失败: {str(e)}")
                        import traceback
                        traceback.print_exc()
                else:
                    print(f"[监听] ⚠️ 跳过发送提醒（should_alert=False）")
                    
                # 自动发送模板消息（异步执行，不阻塞）
                if settings_service.get_start_sending(account['id']):
                    tpl = settings_service.get_template_message(account['id'])
                    if tpl:
                        async def _send_template():
                            try:
                                # 获取延迟配置
                                delay = settings_service.get_send_delay(account['id'])
                                jitter = settings_service.get_send_jitter()
                                if delay > 0:
                                    # 延迟必须在 Task 内部等待，否则会阻塞主消息循环
                                    await asyncio.sleep(max(0.0, delay + random.uniform(-jitter, jitter)))
                                
                                sem = _get_semaphore(account['id'])
                                async with sem:
                                    await event.client.send_message(event.chat_id, tpl)
                            except Exception:
                                pass
                        
                        # 创建后台任务执行发送
                        asyncio.create_task(_send_template())

        # =================================================================
        # 2) 按钮点击（仅当角色包含 click）
        # =================================================================
        if role in ('click', 'both'):
            buttons = getattr(event.message, 'buttons', None)
            if buttons:
                keywords = settings_service.get_account_keywords(account['id'], kind='click') or []
                if keywords:
                    # 遍历按钮，查找匹配
                    for i, row in enumerate(buttons):
                        for j, btn in enumerate(row):
                            btn_text = getattr(btn, 'text', None) or ''
                            if any(k for k in keywords if k and k in btn_text):
                                print(f"[点击] ✅ 账号 #{account['id']} 匹配按钮: '{btn_text}'")
                                
                                # 定义点击任务
                                async def _click_button(row_idx, col_idx, b_text):
                                    try:
                                        # 获取延迟配置
                                        delay = settings_service.get_click_delay(account['id'])
                                        jitter = settings_service.get_click_jitter()
                                        if delay > 0:
                                            # 延迟必须在 Task 内部等待
                                            await asyncio.sleep(max(0.0, delay + random.uniform(-jitter, jitter)))
                                        
                                        # 尝试点击（使用信号量控制并发）
                                        sem = _get_semaphore(account['id'])
                                        async with sem:
                                            print(f"[点击] 账号 #{account['id']} 准备点击按钮 [{row_idx},{col_idx}] '{b_text}'")
                                            await event.click(row_idx, col_idx)
                                            print(f"[点击] ✅ 账号 #{account['id']} 点击成功（按钮：{b_text}）")
                                    except Exception as e:
                                        error_str = str(e)
                                        error_type = type(e).__name__
                                        print(f"[点击] ❌ 账号 #{account['id']} 点击失败：{error_type}: {error_str}")
                                        import traceback
                                        traceback.print_exc()
                                        
                                        # 如果点击失败，退化为发送按钮文本（仅当是回复键盘按钮时）
                                        # 对于内联按钮，不应该发送文本
                                        if 'BUTTON_INVALID' not in error_str and 'INLINE' not in error_str:
                                            try:
                                                print(f"[点击] 尝试发送按钮文本作为备选方案...")
                                                await event.client.send_message(event.chat_id, b_text)
                                                print(f"[点击] ⚠️ 账号 #{account['id']} 点击失败，已转为发送文本 '{b_text}'")
                                            except Exception as e2:
                                                print(f"[点击] ❌ 账号 #{account['id']} 发送文本也失败：{str(e2)}")
                                        else:
                                            print(f"[点击] ⚠️ 账号 #{account['id']} 无法发送文本（内联按钮或按钮无效）")

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
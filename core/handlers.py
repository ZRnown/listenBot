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


async def on_new_message(event, account: dict, bot_client):
    """处理新消息：监听关键词和点击按钮（支持多账号并发）"""
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
        
        # 1) 关键词监听（仅当角色包含 listen）
        if role in ('listen', 'both'):
            keywords = settings_service.get_account_keywords(account['id'], kind='listen') or []
            if keywords:
                matched = match_keywords(account['id'], text, kind='listen')
                if matched:
                    print(f"[监听] ✅ 账号 #{account['id']} 匹配关键词: '{matched}'")
                    target = settings_service.get_target_chat()
                    if target and target.strip() and bot_client:
                        # 检查是否来自机器人
                        try:
                            sender = await event.get_sender()
                            if getattr(sender, 'bot', False):
                                return  # 跳过机器人消息
                        except Exception:
                            pass
                        
                        try:
                            await send_alert(bot_client, account, event, matched)
                        except Exception as e:
                            print(f"[监听] ❌ 发送提醒失败: {str(e)}")
                        
                        # 自动发送模板消息（异步执行，不阻塞）
                        if settings_service.get_start_sending(account['id']):
                            tpl = settings_service.get_template_message(account['id'])
                            if tpl:
                                delay = settings_service.get_send_delay(account['id'])
                                jitter = settings_service.get_send_jitter()
                                async def _send_template():
                                    try:
                                        if delay > 0:
                                            await asyncio.sleep(max(0.0, delay + random.uniform(-jitter, jitter)))
                                        sem = _get_semaphore(account['id'])
                                        async with sem:
                                            await event.client.send_message(event.chat_id, tpl)
                                    except Exception:
                                        pass
                                asyncio.create_task(_send_template())
        
        # 2) 按钮点击（仅当角色包含 click）
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
                                # 应用延迟
                                delay = settings_service.get_click_delay(account['id'])
                                jitter = settings_service.get_click_jitter()
                                if delay > 0:
                                    await asyncio.sleep(max(0.0, delay + random.uniform(-jitter, jitter)))
                                
                                # 尝试点击（异步执行，使用信号量控制并发）
                                async def _click_button():
                                    try:
                                        sem = _get_semaphore(account['id'])
                                        async with sem:
                                            await event.click(i, j)
                                            print(f"[点击] ✅ 账号 #{account['id']} 点击成功")
                                    except Exception as e:
                                        # 退化为发送按钮文本
                                        try:
                                            await event.client.send_message(event.chat_id, btn_text)
                                            print(f"[点击] ✅ 账号 #{account['id']} 发送文本成功")
                                        except Exception:
                                            pass
                                
                                asyncio.create_task(_click_button())
                                return
    except (GeneratorExit, RuntimeError) as e:
        if 'GeneratorExit' in str(type(e).__name__) or 'coroutine ignored' in str(e):
            return
        print(f"[监听] ⚠️ 账号 #{account.get('id', '?')} RuntimeError: {str(e)}")
    except Exception as e:
        print(f"[监听] ❌ 账号 #{account.get('id', '?')} 错误: {str(e)}")

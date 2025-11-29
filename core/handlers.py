import asyncio
import random
from datetime import datetime
from core.filters import match_keywords
from services.alerting import send_alert
from services import settings_service

# per-account concurrency control
_ACCOUNT_SEMAPHORES: dict[int, tuple[asyncio.Semaphore, int]] = {}


def _get_semaphore(account_id: int) -> asyncio.Semaphore:
    value = max(1, settings_service.get_concurrency(account_id))
    sem, current = _ACCOUNT_SEMAPHORES.get(account_id, (None, 0))
    if sem is None or current != value:
        sem = asyncio.Semaphore(value)
        _ACCOUNT_SEMAPHORES[account_id] = (sem, value)
    return sem


async def on_new_message(event, account: dict, bot_client):
    try:
        # bot_client 用于发送监听提醒消息到目标群组
        # 只处理群组消息（使用 event.is_group 判断，包括普通群和超级群）
        print(f"[监听日志] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 📨 账号 #{account['id']} 收到新消息")
        print(f"[监听日志]   私聊: {event.is_private}, 群组: {event.is_group}, 频道: {event.is_channel}")
        
        # 跳过私聊消息
        if event.is_private:
            print(f"[监听日志] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 账号 #{account['id']} 收到私聊消息，跳过处理")
            return
        
        # 只处理群组消息（使用 event.is_group，包括普通群和超级群）
        # 如果需要监听频道，可以改为 if event.is_group or event.is_channel
        if not event.is_group:
            print(f"[监听日志] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 账号 #{account['id']} 收到非群组消息（可能是频道），跳过处理")
            return
        
        # 获取消息文本（包括纯文本和媒体消息的标题/说明）
        text = event.message.message or ''
        # 如果没有文本，尝试获取其他可能的文本内容
        if not text:
            # 尝试获取原始文本
            text = getattr(event.message, 'raw_text', '') or ''
            # 尝试获取消息的文本属性
            if not text:
                text = str(event.message.text) if hasattr(event.message, 'text') else ''
        
        # 获取群组信息用于日志
        try:
            chat = await event.get_chat()
            chat_id = event.chat_id
            chat_type = type(chat).__name__
            chat_title = getattr(chat, 'title', '') or getattr(chat, 'username', '') or f"Chat#{chat_id}"
            chat_username = getattr(chat, 'username', None)
            is_megagroup = getattr(chat, 'megagroup', False)
            is_broadcast = getattr(chat, 'broadcast', False)
            print(f"[监听日志] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 账号 #{account['id']} 在群组 '{chat_title}' (ID: {chat_id}, 类型: {chat_type}, 用户名: {chat_username}, 超级群: {is_megagroup}, 频道: {is_broadcast}) 收到消息")
        except Exception as e:
            chat_title = f"Chat#{event.chat_id}"
            print(f"[监听日志] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 账号 #{account['id']} 在群组 '{chat_title}' 收到消息 (获取群组信息失败: {str(e)})")
        
        # 显示完整的消息文本（用于调试）
        text_display = repr(text) if text else "''"
        print(f"[监听日志] 消息文本 (repr): {text_display}")
        print(f"[监听日志] 消息文本 (显示): '{text}' (长度: {len(text)})")
        
        role = settings_service.get_account_role(account['id']) or 'both'
        print(f"[监听日志] 账号 #{account['id']} 的角色: {role}")

        # 1) 关键词提醒（仅当角色包含 listen）
        if role in ('listen', 'both'):
            print(f"[监听日志] 账号 #{account['id']} 角色包含 listen，开始检查关键词")
            # 监听账号监听所有群组（不再使用监听源过滤）
            
            # 检查关键词匹配
            keywords = settings_service.get_account_keywords(account['id'], kind='listen') or []
            print(f"[监听日志] 账号 #{account['id']} 的监听关键词列表: {keywords}")
            if not keywords:
                print(f"[监听日志] 账号 #{account['id']} 没有设置监听关键词，跳过处理")
                return
            
            # 详细显示匹配过程
            print(f"[监听日志] 开始匹配关键词...")
            for kw in keywords:
                kw_clean = kw.strip() if kw else ''
                if kw_clean:
                    in_text = kw_clean in text
                    print(f"[监听日志]   检查关键词 '{kw_clean}' (repr: {repr(kw_clean)}) 是否在文本中: {in_text}")
                    if in_text:
                        print(f"[监听日志]   ✅ 找到匹配: '{kw_clean}'")
            
            matched = match_keywords(account['id'], text, kind='listen')
            print(f"[监听日志] 关键词匹配结果: {matched if matched else '未匹配'}")
            if matched:
                print(f"[监听日志] ✅ 匹配到关键词: '{matched}'")
                # 使用全局转发目标
                target = settings_service.get_target_chat()
                print(f"[监听日志] 全局转发目标: {target if target else '未设置'}")
                if not target or not target.strip():
                    print(f"[监听日志] ❌ 转发目标未设置，跳过发送")
                    return
                
                # 如果已设置转发目标，过滤掉机器人发送的消息
                try:
                    sender = await event.get_sender()
                    is_bot = getattr(sender, 'bot', False)
                    if is_bot:
                        print(f"[监听日志] ⚠️ 消息来自机器人，已设置转发目标，跳过处理")
                        return
                except Exception:
                    # 如果获取发送者失败，继续处理
                    pass
                
                # 使用机器人客户端发送监听信息
                if not bot_client:
                    print(f"[监听日志] ❌ bot_client 为空，无法发送提醒")
                    return
                
                print(f"[监听日志] 准备发送提醒到目标: {target}")
                try:
                    await send_alert(bot_client, account, event, matched)
                    print(f"[监听日志] ✅ 提醒发送成功")
                except Exception as e:
                    print(f"[监听日志] ❌ 发送提醒失败: {str(e)}")
                    import traceback
                    traceback.print_exc()
                
                # optional: start sending template message
                if settings_service.get_start_sending(account['id']):
                    tpl = settings_service.get_template_message(account['id'])
                    if tpl:
                        delay = settings_service.get_send_delay(account['id'])
                        jitter = settings_service.get_send_jitter()
                        async def _send():
                            try:
                                if delay and delay > 0:
                                    # add small random jitter to avoid patterns
                                    await asyncio.sleep(max(0.0, delay + random.uniform(-jitter, jitter)))
                                await event.client.send_message(event.chat_id, tpl)
                            except Exception:
                                pass
                        sem = _get_semaphore(account['id'])
                        async def _runner():
                            async with sem:
                                await _send()
                        # do not block handler
                        asyncio.create_task(_runner())
            else:
                print(f"[监听日志] 消息文本 '{text}' 未匹配任何关键词")
        else:
            print(f"[监听日志] 账号 #{account['id']} 角色为 '{role}'，不包含 listen，跳过监听处理")

        # 2) 按钮点击（仅当角色包含 click）
        if role not in ('click', 'both'):
            return
        buttons = getattr(event.message, 'buttons', None)
        if not buttons:
            return
        # buttons is List[List[Button]]
        keywords = settings_service.get_account_keywords(account['id'], kind='click') or []
        if not keywords:
            return
        # 遍历按钮，查找命中
        print(f"[点击功能] 账号 #{account['id']}: 检查按钮，关键词列表: {keywords}")
        for i, row in enumerate(buttons):
            for j, btn in enumerate(row):
                btn_text = getattr(btn, 'text', None) or ''
                btn_type = type(btn).__name__
                print(f"[点击功能] 账号 #{account['id']}: 检查按钮 [{i},{j}] '{btn_text}' (类型: {btn_type})")
                if any(k for k in keywords if k and k in btn_text):
                    print(f"[点击功能] ✅ 账号 #{account['id']}: 匹配到关键词，准备点击按钮 '{btn_text}'")
                    # 点击延迟
                    delay = settings_service.get_click_delay(account['id'])
                    jitter = settings_service.get_click_jitter()
                    if delay and delay > 0:
                        sleep_time = max(0.0, delay + random.uniform(-jitter, jitter))
                        print(f"[点击功能] 账号 #{account['id']}: 等待 {sleep_time:.2f} 秒后点击")
                        await asyncio.sleep(sleep_time)
                    # 判定 Inline vs Reply 按钮
                    try:
                        # 优先尝试 inline 点击（有 callback 的）
                        print(f"[点击功能] 账号 #{account['id']}: 尝试点击按钮 [{i},{j}]")
                        await event.click(i, j)
                        print(f"[点击功能] ✅ 账号 #{account['id']}: 点击成功（按钮：{btn_text}）")
                    except Exception as e:
                        print(f"[点击功能] ⚠️ 账号 #{account['id']}: inline点击失败 ({str(e)})，尝试发送按钮文本")
                        # 退化为发送按钮文本（reply keyboard）
                        try:
                            await event.client.send_message(event.chat_id, btn_text)
                            print(f"[点击功能] ✅ 账号 #{account['id']}: 发送按钮文本成功（按钮：{btn_text}）")
                        except Exception as e2:
                            print(f"[点击功能] ❌ 账号 #{account['id']}: 发送按钮文本也失败: {str(e2)}")
                    return
        print(f"[点击功能] 账号 #{account['id']}: 未找到匹配关键词的按钮")
    except (GeneratorExit, RuntimeError) as e:
        # 忽略 Telethon 内部连接关闭时的错误
        if 'GeneratorExit' in str(type(e).__name__) or 'coroutine ignored' in str(e):
            return
        # 其他错误打印日志
        print(f"[监听日志] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⚠️ 账号 #{account.get('id', '?')} 处理消息时发生 RuntimeError: {str(e)}")
    except Exception as e:
        # 打印错误以便调试
        print(f"[监听日志] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ❌ 账号 #{account.get('id', '?')} 处理消息时发生未预期错误: {str(e)}")
        import traceback
        traceback.print_exc()

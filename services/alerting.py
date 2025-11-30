import asyncio
from telethon import events
from telethon.tl.types import PeerUser, PeerChat, PeerChannel
from telethon.tl.custom import Button
from storage import dao_alerts
from services import settings_service

# 全局发送队列，确保所有发送任务真正并发
_send_queue = asyncio.Queue()
_send_workers_started = False
_send_workers = []

async def _send_worker(worker_id: int):
    """发送工作协程：从队列中取出任务并发送，确保真正并发"""
    while True:
        try:
            # 从队列中获取发送任务
            task_data = await _send_queue.get()
            if task_data is None:  # 停止信号
                break
            
            # task_data 现在是一个包含所有信息的字典，需要在这里完成所有耗时操作
            bot_client = task_data['bot_client']
            event = task_data['event']
            account = task_data['account']
            matched_keyword = task_data['matched_keyword']
            control_bot_id = task_data.get('control_bot_id')
            
            # 在后台工作协程中完成所有耗时操作（真正并发）
            try:
                # 并发获取 sender 和 chat（使用更短的超时，快速失败）
                try:
                    sender, chat = await asyncio.wait_for(
                        asyncio.gather(
                            event.get_sender(),
                            event.get_chat(),
                            return_exceptions=True
                        ),
                        timeout=0.2  # 缩短超时到0.2秒，快速失败
                    )
                    if isinstance(sender, Exception):
                        sender = None
                    if isinstance(chat, Exception):
                        chat = None
                except asyncio.TimeoutError:
                    sender = None
                    chat = None
                
                # 快速检查：如果消息来自控制机器人，跳过发送
                if sender:
                    sender_id = getattr(sender, 'id', None)
                    is_bot = getattr(sender, 'bot', False)
                    if is_bot and control_bot_id and sender_id == control_bot_id:
                        # 跳过发送，但记录到数据库
                        _record_alert_async(account, event, matched_keyword, sender, chat, 'error', '消息来自控制机器人')
                        _send_queue.task_done()
                        continue
                
                # 安全获取信息
                sender_name = 'Unknown'
                sender_username = None
                sender_id = None
                if sender:
                    sender_name = f"{getattr(sender,'first_name', '') or ''} {getattr(sender,'last_name','') or ''}".strip() or 'Unknown'
                    sender_username = getattr(sender, 'username', None)
                    sender_id = getattr(sender, 'id', None)
                
                sender_username_display = f"@{sender_username}" if sender_username else '无'
                source_title = (getattr(chat, 'title', '') or getattr(chat, 'username','') or 'Unknown') if chat else 'Unknown'
                text = event.message.message or ''
                source_chat_id = getattr(chat, 'id', None) if chat else None
                
                # 快速获取 chat_entity（不阻塞）
                chat_username = getattr(chat, 'username', None) if chat else None
                
                # 构建消息内容
                account_id = account['id']
                account_username = account.get('username')
                if account_username:
                    account_display = f"@{account_username}"
                else:
                    account_display = account.get('phone') or f"#{account_id}"
                
                # 转义Markdown特殊字符
                def escape_md(text):
                    if not text:
                        return ''
                    text = str(text)
                    text = text.replace('\\', '\\\\')
                    text = text.replace('*', '\\*')
                    text = text.replace('_', '\\_')
                    text = text.replace('[', '\\[')
                    text = text.replace(']', '\\]')
                    text = text.replace('(', '\\(')
                    text = text.replace(')', '\\)')
                    text = text.replace('`', '\\`')
                    return text
                
                message_text = (
                    f"🔔 **关键词提醒**\n\n"
                    f"📱 **监听账号：** `{escape_md(account_display)}`\n"
                    f"🔑 **关键字：** `{escape_md(matched_keyword)}`\n"
                    f"👤 **发送者：** {escape_md(sender_name)}\n"
                    f"📝 **用户名：** {escape_md(sender_username_display)}\n"
                    f"💬 **来源群组：** `{escape_md(source_title)}`\n"
                    f"📄 **消息内容：** {escape_md(text)}"
                )
                
                # 快速生成消息链接
                buttons = []
                msg_link = None
                if source_chat_id and event.message.id:
                    if chat_username:
                        msg_link = f"https://t.me/{chat_username}/{event.message.id}"
                    elif str(source_chat_id).startswith('-100'):
                        channel_id = str(source_chat_id)[4:]
                        if channel_id.isdigit():
                            msg_link = f"https://t.me/c/{channel_id}/{event.message.id}"
                    elif str(source_chat_id).startswith('-'):
                        msg_link = f"tg://openmessage?chat_id={source_chat_id}&message_id={event.message.id}"
                    else:
                        msg_link = f"https://t.me/c/{source_chat_id}/{event.message.id}"
                
                if msg_link and (msg_link.startswith('https://') or msg_link.startswith('tg://')):
                    buttons.append([Button.url('👁️ 查看消息', msg_link)])
                
                if sender_id:
                    buttons.append([Button.inline('🚫 屏蔽该用户', data=f'block_user:{sender_id}')])
                
                # 获取目标实体
                target = settings_service.get_target_chat()
                if not target or not target.strip():
                    _record_alert_async(account, event, matched_keyword, sender, chat, 'error', 'Target chat not configured')
                    _send_queue.task_done()
                    continue
                
                target_clean = target.strip()
                is_chat_id = False
                chat_id_int = None
                try:
                    chat_id_int = int(target_clean)
                    is_chat_id = True
                except (ValueError, AttributeError):
                    if not target_clean.startswith('@'):
                        if not target_clean.startswith('http'):
                            target_clean = '@' + target_clean.lstrip('@')
                
                target_entity = chat_id_int if is_chat_id else target_clean
                
                # 立即发送消息（这是真正的发送操作）
                try:
                    await bot_client.send_message(
                        target_entity,
                        message_text,
                        parse_mode='markdown',
                        buttons=buttons if buttons else None
                    )
                    # 后台记录成功（不阻塞）
                    _record_alert_async(account, event, matched_keyword, sender, chat, 'success', None)
                except Exception as send_error:
                    error_str = str(send_error)
                    error_type = type(send_error).__name__
                    # 后台记录失败（不阻塞）
                    _record_alert_async(account, event, matched_keyword, sender, chat, 'error', error_str[:200])
            
            except Exception as e:
                # 记录错误但不阻塞
                _record_alert_async(account, event, matched_keyword, None, None, 'error', str(e)[:200])
            
            _send_queue.task_done()
        except Exception as e:
            print(f"[发送工作协程 #{worker_id}] ❌ 错误: {e}")

def _record_alert_async(account, event, matched_keyword, sender, chat, delivered_status, delivered_error):
    """异步记录提醒到数据库（不阻塞）"""
    def _record():
        try:
            sender_name = 'Unknown'
            sender_username = None
            sender_id = None
            if sender:
                sender_name = f"{getattr(sender,'first_name', '') or ''} {getattr(sender,'last_name','') or ''}".strip() or 'Unknown'
                sender_username = getattr(sender, 'username', None)
                sender_id = getattr(sender, 'id', None)
            
            source_title = (getattr(chat, 'title', '') or getattr(chat, 'username','') or 'Unknown') if chat else 'Unknown'
            text = event.message.message or ''
            source_chat_id = getattr(chat, 'id', None) if chat else None
            
            dao_alerts.insert_alert(
                account_id=account['id'],
                source_chat_id=source_chat_id,
                source_chat_title=source_title,
                sender_id=sender_id,
                sender_name=sender_name,
                sender_username=sender_username,
                message_text=text,
                matched_keyword=matched_keyword,
                delivered_status=delivered_status,
                delivered_error=delivered_error,
            )
        except Exception:
            pass  # 忽略数据库错误，不影响发送
    
    # 在后台线程中执行（不阻塞事件循环）
    asyncio.get_event_loop().run_in_executor(None, _record)

def _ensure_send_workers(bot_client):
    """确保发送工作协程已启动（全局共享，所有 bot_client 使用同一个协程池）"""
    global _send_workers_started, _send_workers
    if not _send_workers_started:
        # 大幅增加工作协程数量，确保极致并发
        num_workers = 500  # 增加到500个并发工作协程
        _send_workers = [asyncio.create_task(_send_worker(i)) for i in range(num_workers)]
        _send_workers_started = True
        print(f"[发送队列] 启动 {num_workers} 个发送工作协程，确保真正并发")

async def send_alert(bot_client, account, event, matched_keyword: str, control_bot_id=None):
    """快速发送提醒：立即放入队列，所有耗时操作在工作协程中完成（真正并发）"""
    # 确保发送工作协程已启动
    _ensure_send_workers(bot_client)
    
    # 立即将任务放入队列，不等待任何操作
    # 所有耗时操作（获取sender、chat、构建消息、发送、记录数据库）都在工作协程中完成
    await _send_queue.put({
        'bot_client': bot_client,
        'event': event,
        'account': account,
        'matched_keyword': matched_keyword,
        'control_bot_id': control_bot_id,
    })
    
    # 立即返回，不等待任何操作完成
    # 这样多个消息可以真正并发处理

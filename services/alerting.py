import asyncio
from datetime import datetime
from telethon import events
from telethon.tl.types import PeerUser, PeerChat, PeerChannel
from telethon.tl.custom import Button
from storage import dao_alerts
from services import settings_service

# Global send queue to ensure concurrent sending
_send_queue = asyncio.Queue()
_send_workers_started = False
_send_workers = []

async def _send_worker(worker_id: int):
    """Worker coroutine: retrieves tasks from the queue and sends them concurrently."""
    while True:
        try:
            # Get task from queue
            task_data = await _send_queue.get()
            if task_data is None:  # Stop signal
                break
            
            bot_client, target_entity, message_text, buttons, parse_mode = task_data
            
            # Send immediately
            try:
                await bot_client.send_message(
                    target_entity,
                    message_text,
                    parse_mode=parse_mode,
                    buttons=buttons
                )
            except Exception as e:
                print(f"[Send Worker #{worker_id}] ❌ Send failed: {e}")
            
            _send_queue.task_done()
        except Exception as e:
            print(f"[Send Worker #{worker_id}] ❌ Error: {e}")

def _ensure_send_workers(bot_client):
    """Ensure send workers are started (shared globally)."""
    global _send_workers_started, _send_workers
    if not _send_workers_started:
        # Start multiple workers for concurrency
        num_workers = 200  # High concurrency for high volume
        _send_workers = [asyncio.create_task(_send_worker(i)) for i in range(num_workers)]
        _send_workers_started = True
        print(f"[Send Queue] Started {num_workers} send workers")

async def send_alert(bot_client, account, event, matched_keyword: str, control_bot_id=None):
    """Send alert: Fully asynchronous, non-blocking, queues immediately."""
    
    # Get basic info immediately
    text = event.message.message or ''
    msg_id = getattr(event.message, 'id', None)
    chat_id = getattr(event, 'chat_id', None)
    
    # Async task to get sender and chat info without blocking
    async def _get_info():
        try:
            # Wait with timeout
            sender, chat = await asyncio.wait_for(
                asyncio.gather(
                    event.get_sender(),
                    event.get_chat(),
                    return_exceptions=True
                ),
                timeout=1.0
            )
            
            if isinstance(sender, Exception): sender = None
            if isinstance(chat, Exception): chat = None
            
            # Fallback for sender
            if sender is None:
                try:
                    if hasattr(event, 'sender') and event.sender:
                        sender = event.sender
                    elif hasattr(event.message, 'sender') and event.message.sender:
                        sender = event.message.sender
                    elif hasattr(event.message, 'sender_id') and event.message.sender_id:
                        try:
                            sender = await event.client.get_entity(event.message.sender_id)
                        except: pass
                except: pass
            
            # Fallback for chat
            if chat is None:
                try:
                    if hasattr(event, 'chat') and event.chat:
                        chat = event.chat
                except: pass
            
            # Check if sender is the control bot to skip
            if sender:
                sender_id = getattr(sender, 'id', None)
                is_bot = getattr(sender, 'bot', False)
                if is_bot and control_bot_id and sender_id == control_bot_id:
                    return None, None, True  # True = Skip
            
            return sender, chat, False
        except asyncio.TimeoutError:
            return None, None, False
        except Exception as e:
            print(f"[Send Alert] Info fetch error: {e}")
            return None, None, False
    
    # Start info fetch task
    info_task = asyncio.create_task(_get_info())
    
    # Prepare account display info
    account_id = account['id']
    account_username = account.get('username')
    account_display = f"@{account_username}" if account_username else (account.get('phone') or f"#{account_id}")
    
    # Wait for info with timeout
    try:
        sender, chat, should_skip = await asyncio.wait_for(info_task, timeout=1.5)
        if should_skip:
            return
    except (asyncio.TimeoutError, Exception):
        sender = None
        chat = None

    # Process sender info
    sender_name = 'Unknown'
    sender_username = None
    sender_id = None
    
    if not sender and hasattr(event, 'sender_id') and event.sender_id:
        sender_id = event.sender_id
        try:
            sender = await event.client.get_entity(sender_id)
        except: pass

    if sender:
        if hasattr(sender, 'title'):
            sender_name = sender.title
        elif hasattr(sender, 'first_name') or hasattr(sender, 'last_name'):
            first = getattr(sender, 'first_name', '') or ''
            last = getattr(sender, 'last_name', '') or ''
            sender_name = f"{first} {last}".strip() or 'Unknown'
        else:
            sender_name = str(sender)
        
        sender_username = getattr(sender, 'username', None)
        sender_id = getattr(sender, 'id', None) or sender_id
    
    # Process chat info
    if not chat and chat_id:
        try:
            chat = await event.client.get_entity(chat_id)
        except: pass
    
    sender_username_display = f"@{sender_username}" if sender_username else '无'
    source_title = (getattr(chat, 'title', '') or getattr(chat, 'username','') or 'Unknown') if chat else 'Unknown'
    source_chat_id = getattr(chat, 'id', None) if chat else chat_id

    # Get target
    target = settings_service.get_target_chat()
    delivered = 'error'
    error = None

    if not target or not target.strip():
        error = 'Target chat not configured'
    else:
        try:
            target_clean = target.strip()
            
            if any(target_clean.startswith(p) for p in ['https://t.me/+', 'https://t.me/joinchat/', 't.me/+', 't.me/joinchat/']):
                raise ValueError('Forward target cannot be an invite link.')
            
            is_chat_id = False
            chat_id_int = None
            try:
                chat_id_int = int(target_clean)
                is_chat_id = True
            except (ValueError, AttributeError):
                pass
            
            if not is_chat_id and not target_clean.startswith('@') and not target_clean.startswith('http'):
                target_clean = '@' + target_clean.lstrip('@')
            
            # Helper to escape Markdown
            def escape_md(text):
                if not text: return ''
                return str(text).replace('\\', '\\\\').replace('*', '\\*').replace('_', '\\_').replace('[', '\\[').replace(']', '\\]').replace('(', '\\(').replace(')', '\\)').replace('`', '\\`')
            
            message_text = (
                f"🔔 **关键词提醒**\n\n"
                f"📱 **监听账号：** `{escape_md(account_display)}`\n"
                f"🔑 **关键字：** `{escape_md(matched_keyword)}`\n"
                f"👤 **发送者：** {escape_md(sender_name)}\n"
                f"📝 **用户名：** {escape_md(sender_username_display)}\n"
                f"💬 **来源群组：** `{escape_md(source_title)}`\n"
                f"📄 **消息内容：** {escape_md(text)}"
            )
            
            # Buttons
            buttons = []
            msg_link = None
            if source_chat_id and msg_id:
                try:
                    if chat and getattr(chat, 'username', None):
                        msg_link = f"https://t.me/{chat.username}/{msg_id}"
                    else:
                        cid_str = str(source_chat_id)
                        if cid_str.startswith('-100'):
                            msg_link = f"https://t.me/c/{cid_str[4:]}/{msg_id}"
                        else:
                            msg_link = f"tg://openmessage?chat_id={source_chat_id}&message_id={msg_id}"
                except: pass
            
            if not msg_link and source_chat_id and msg_id:
                 msg_link = f"tg://openmessage?chat_id={source_chat_id}&message_id={msg_id}"
            
            btn_row = []
            if msg_link and (msg_link.startswith('https://') or msg_link.startswith('tg://')):
                btn_row.append(Button.url('👁️ 查看消息', msg_link))
            if btn_row:
                buttons.append(btn_row)
            
            if sender_id:
                buttons.append([Button.inline('🚫 屏蔽该用户', data=f'block_user:{sender_id}')])
            
            # Enqueue
            target_entity = chat_id_int if is_chat_id else target_clean
            _ensure_send_workers(bot_client)
            _send_queue.put_nowait((
                bot_client,
                target_entity,
                message_text,
                buttons if buttons else None,
                'markdown'
            ))
            
            delivered = 'success'
        except ValueError as ve:
            error = str(ve)
        except Exception as e:
            error = f"Failed to send: {str(e)[:200]}"

    # Log alert
    dao_alerts.insert_alert(
        account_id=account['id'],
        source_chat_id=source_chat_id,
        source_chat_title=source_title,
        sender_id=sender_id,
        sender_name=sender_name,
        sender_username=sender_username,
        message_text=text,
        matched_keyword=matched_keyword,
        delivered_status=delivered,
        delivered_error=error,
    )
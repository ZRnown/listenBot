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
                # 第一步：立即从消息对象中获取可用信息（不等待API调用）
                # 这样可以立即发送基本消息，后台再补充完整信息
                text = event.message.message or ''
                if not text:
                    text = getattr(event.message, 'raw_text', '') or ''
                if not text:
                    text = str(event.message.text) if hasattr(event.message, 'text') else ''
                
                # 从消息对象中直接获取chat_id（通常可用）
                source_chat_id = getattr(event, 'chat_id', None)
                if not source_chat_id:
                    source_chat_id = getattr(event.message, 'peer_id', None)
                    if hasattr(source_chat_id, 'channel_id'):
                        source_chat_id = -1000000000000 - source_chat_id.channel_id
                
                # 从消息对象中直接获取sender_id（如果可用）
                sender_id_from_msg = None
                if hasattr(event.message, 'from_id'):
                    from_id = event.message.from_id
                    if from_id:
                        if hasattr(from_id, 'user_id'):
                            sender_id_from_msg = from_id.user_id
                        elif isinstance(from_id, int):
                            sender_id_from_msg = from_id
                
                # 快速检查：如果sender_id匹配控制机器人，跳过发送
                if sender_id_from_msg and control_bot_id and sender_id_from_msg == control_bot_id:
                    _record_alert_async(account, event, matched_keyword, None, None, 'error', '消息来自控制机器人')
                    _send_queue.task_done()
                    continue
                
                # 使用默认值，先发送基本消息（使用可用信息）
                sender_name = f'用户 #{sender_id_from_msg}' if sender_id_from_msg else 'Unknown'
                sender_username = None
                sender_id = sender_id_from_msg
                sender_username_display = '无'
                source_title = f'群组 #{source_chat_id}' if source_chat_id else 'Unknown'
                chat_username = None
                
                # 第二步：后台异步获取完整信息（不阻塞发送）
                async def _fetch_full_info():
                    """后台获取完整信息，用于后续更新或记录"""
                    try:
                        sender, chat = await asyncio.wait_for(
                            asyncio.gather(
                                event.get_sender(),
                                event.get_chat(),
                                return_exceptions=True
                            ),
                            timeout=2.0  # 增加到2秒，确保能获取到信息
                        )
                        if isinstance(sender, Exception):
                            sender = None
                        if isinstance(chat, Exception):
                            chat = None
                        
                        # 更新信息（用于数据库记录）
                        if sender:
                            sender_name_full = f"{getattr(sender,'first_name', '') or ''} {getattr(sender,'last_name','') or ''}".strip() or 'Unknown'
                            sender_username_full = getattr(sender, 'username', None)
                            sender_id_full = getattr(sender, 'id', None)
                        else:
                            sender_name_full = 'Unknown'
                            sender_username_full = None
                            sender_id_full = sender_id_from_msg
                        
                        source_title_full = (getattr(chat, 'title', '') or getattr(chat, 'username','') or 'Unknown') if chat else source_title
                        chat_username_full = getattr(chat, 'username', None) if chat else None
                        
                        return {
                            'sender_name': sender_name_full,
                            'sender_username': sender_username_full,
                            'sender_id': sender_id_full,
                            'source_title': source_title_full,
                            'chat_username': chat_username_full,
                            'chat': chat
                        }
                    except asyncio.TimeoutError:
                        return None
                    except Exception:
                        return None
                
                # 启动后台任务获取完整信息（不等待）
                full_info_task = asyncio.create_task(_fetch_full_info())
                
                # 立即使用可用信息构建消息（不等待完整信息）
                # 如果获取到完整信息，会在数据库记录中使用
                
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
                
                # 快速生成消息链接（使用可用信息）
                buttons = []
                msg_link = None
                msg_id = getattr(event.message, 'id', None)
                if source_chat_id and msg_id:
                    # 先尝试使用tg://协议（总是可用）
                    msg_link = f"tg://openmessage?chat_id={source_chat_id}&message_id={msg_id}"
                
                if msg_link:
                    buttons.append([Button.url('👁️ 查看消息', msg_link)])
                
                if sender_id:
                    buttons.append([Button.inline('🚫 屏蔽该用户', data=f'block_user:{sender_id}')])
                
                # 获取目标实体
                target = settings_service.get_target_chat()
                if not target or not target.strip():
                    # 等待完整信息后再记录
                    full_info = await full_info_task if not full_info_task.done() else None
                    _record_alert_async(account, event, matched_keyword, 
                                       full_info.get('chat') if full_info else None,
                                       full_info if full_info else None,
                                       'error', 'Target chat not configured')
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
                
                # 立即发送消息（使用基本信息，不等待完整信息）
                try:
                    await bot_client.send_message(
                        target_entity,
                        message_text,
                        parse_mode='markdown',
                        buttons=buttons if buttons else None
                    )
                    # 等待完整信息后再记录（但发送已完成）
                    full_info = await full_info_task if not full_info_task.done() else None
                    if full_info:
                        # 使用完整信息记录
                        _record_alert_async(account, event, matched_keyword,
                                           full_info.get('chat'),
                                           full_info,
                                           'success', None)
                    else:
                        # 使用基本信息记录
                        _record_alert_async(account, event, matched_keyword, None, None, 'success', None)
                except Exception as send_error:
                    error_str = str(send_error)
                    # 等待完整信息后再记录
                    full_info = await full_info_task if not full_info_task.done() else None
                    _record_alert_async(account, event, matched_keyword,
                                       full_info.get('chat') if full_info else None,
                                       full_info if full_info else None,
                                       'error', error_str[:200])
            
            except Exception as e:
                # 记录错误但不阻塞
                _record_alert_async(account, event, matched_keyword, None, None, 'error', str(e)[:200])
            
            _send_queue.task_done()
        except Exception as e:
            print(f"[发送工作协程 #{worker_id}] ❌ 错误: {e}")

def _record_alert_async(account, event, matched_keyword, chat, full_info, delivered_status, delivered_error):
    """异步记录提醒到数据库（不阻塞）"""
    def _record():
        try:
            # 优先使用完整信息
            if full_info and isinstance(full_info, dict):
                sender_name = full_info.get('sender_name', 'Unknown')
                sender_username = full_info.get('sender_username')
                sender_id = full_info.get('sender_id')
                source_title = full_info.get('source_title', 'Unknown')
                chat_obj = full_info.get('chat')
            else:
                # 使用基本信息
                sender_name = 'Unknown'
                sender_username = None
                sender_id = None
                source_title = 'Unknown'
                chat_obj = chat
            
            # 从chat对象获取信息
            if chat_obj:
                source_title = (getattr(chat_obj, 'title', '') or getattr(chat_obj, 'username','') or source_title)
                source_chat_id = getattr(chat_obj, 'id', None)
            else:
                # 从event获取
                source_chat_id = getattr(event, 'chat_id', None)
                if not source_chat_id:
                    source_chat_id = getattr(event.message, 'peer_id', None)
                    if hasattr(source_chat_id, 'channel_id'):
                        source_chat_id = -1000000000000 - source_chat_id.channel_id
            
            text = event.message.message or ''
            if not text:
                text = getattr(event.message, 'raw_text', '') or ''
            if not text:
                text = str(event.message.text) if hasattr(event.message, 'text') else ''
            
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

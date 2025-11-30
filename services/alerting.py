import asyncio
from telethon import events
from telethon.tl.types import PeerUser, PeerChat, PeerChannel
from telethon.tl.custom import Button
from storage import dao_alerts
from services import settings_service

# 全局发送队列，确保所有发送任务真正并发
_alert_queue = asyncio.Queue()  # 用于 alert 消息
_send_queue = asyncio.Queue()  # 用于普通发送
_alert_workers_started = False
_alert_workers = []
_send_workers_started = False
_send_workers = []

async def _send_worker(worker_id: int):
    """发送工作协程：从队列中取出任务并发送，确保真正并发"""
    from datetime import datetime
    while True:
        try:
            # 从队列中获取发送任务
            task_data = await _send_queue.get()
            if task_data is None:  # 停止信号
                break
            
            bot_client, target_entity, message_text, buttons, parse_mode = task_data
            
            # 立即发送，不等待
            try:
                await bot_client.send_message(
                    target_entity,
                    message_text,
                    parse_mode=parse_mode,
                    buttons=buttons
                )
                # 减少日志输出，只在出错时打印
            except Exception as e:
                print(f"[发送工作协程 #{worker_id}] ❌ 发送失败: {e}")
            
            _send_queue.task_done()
        except Exception as e:
            print(f"[发送工作协程 #{worker_id}] ❌ 错误: {e}")

def _ensure_send_workers(bot_client):
    """确保发送工作协程已启动（全局共享，所有 bot_client 使用同一个协程池）"""
    global _send_workers_started, _send_workers
    if not _send_workers_started:
        # 启动多个工作协程，确保并发发送（增加工作协程数量以提升并发度）
        num_workers = 200  # 200个并发工作协程，确保极致并发
        _send_workers = [asyncio.create_task(_send_worker(i)) for i in range(num_workers)]
        _send_workers_started = True
        print(f"[发送队列] 启动 {num_workers} 个发送工作协程，确保真正并发")

def quick_enqueue_alert(bot_client, account, event, matched_keyword: str, control_bot_id=None):
    """
    极速入队：立即将alert任务放入队列，不阻塞，不等待任何网络请求
    
    这是生产者-消费者模型的生产者端：
    - 生产者（这里）：发现关键词 -> 立即入队 -> 立即返回（0ms延迟）
    - 消费者（_alert_worker）：从队列取任务 -> 获取详细信息 -> 发送消息
    """
    try:
        # 立即入队，不等待，不阻塞
        _alert_queue.put_nowait({
            'bot_client': bot_client,
            'account': account,
            'event': event,
            'matched_keyword': matched_keyword,
            'control_bot_id': control_bot_id
        })
        
        # 确保worker已启动
        _ensure_alert_workers(bot_client)
        
        # 添加调试日志
        account_id = account.get('id', '?')
        msg_id = getattr(event.message, 'id', None)
        print(f"[入队] ✅ 账号 #{account_id} 匹配关键词 '{matched_keyword}' 已入队 (消息ID: {msg_id})")
    except Exception as e:
        print(f"[入队] ❌ 入队失败: {e}")
        import traceback
        traceback.print_exc()


async def _alert_worker(worker_id: int):
    """
    Alert工作协程：从队列中取出任务，获取详细信息并发送
    
    这是生产者-消费者模型的消费者端：
    - 所有耗时的网络请求（get_sender, get_chat, send_message）都在这里执行
    - 即使这里慢一点，也不会影响消息监听的响应速度
    """
    from datetime import datetime
    while True:
        try:
            # 从队列中获取任务
            task_data = await _alert_queue.get()
            if task_data is None:  # 停止信号
                break
            
            bot_client = task_data['bot_client']
            account = task_data['account']
            event = task_data['event']
            matched_keyword = task_data['matched_keyword']
            control_bot_id = task_data.get('control_bot_id')
            
            # 现在才进行耗时的网络请求（即使这里慢，也不影响监听速度）
            await _process_and_send_alert(bot_client, account, event, matched_keyword, control_bot_id)
            
            _alert_queue.task_done()
        except Exception as e:
            print(f"[Alert工作协程 #{worker_id}] ❌ 错误: {e}")


def _ensure_alert_workers(bot_client):
    """确保alert工作协程已启动"""
    global _alert_workers_started, _alert_workers
    if not _alert_workers_started:
        num_workers = 200  # 200个并发worker
        _alert_workers = [asyncio.create_task(_alert_worker(i)) for i in range(num_workers)]
        _alert_workers_started = True
        print(f"[Alert队列] ✅ 启动 {num_workers} 个alert工作协程")


async def _process_and_send_alert(bot_client, account, event, matched_keyword: str, control_bot_id=None):
    """
    处理并发送alert（在worker中执行，不阻塞监听）
    """
    from datetime import datetime
    
    # 零IO获取基本信息
    text = getattr(event.message, 'raw_text', '') or ''
    if not text:
        text = event.message.message or ''
    msg_id = getattr(event.message, 'id', None)
    chat_id = getattr(event, 'chat_id', None)
    
    # 现在才进行耗时的网络请求
    try:
        sender, chat = await asyncio.wait_for(
            asyncio.gather(
                event.get_sender(),
                event.get_chat(),
                return_exceptions=True
            ),
            timeout=0.5  # 0.5秒超时
        )
        
        # 处理异常
        if isinstance(sender, Exception):
            sender = None
        if isinstance(chat, Exception):
            chat = None
        
        # 快速检查：如果消息来自控制机器人，跳过发送
        if sender:
            sender_id = getattr(sender, 'id', None)
            is_bot = getattr(sender, 'bot', False)
            if is_bot and control_bot_id and sender_id == control_bot_id:
                return  # 跳过发送
    except asyncio.TimeoutError:
        sender = None
        chat = None
    except Exception:
        sender = None
        chat = None
    
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
    source_chat_id = getattr(chat, 'id', None) if chat else chat_id

    # 使用全局转发目标
    target = settings_service.get_target_chat()
    if not target or not target.strip():
        delivered = 'error'
        error = 'Target chat not configured'
    else:
        try:
            # 处理转发目标格式
            target_clean = target.strip()
            
            # 检查是否是邀请链接
            if target_clean.startswith('https://t.me/+') or target_clean.startswith('https://t.me/joinchat/') or target_clean.startswith('t.me/+') or target_clean.startswith('t.me/joinchat/'):
                raise ValueError('转发目标不能是邀请链接')
            
            # 检查是否是 Chat ID
            is_chat_id = False
            chat_id_int = None
            try:
                test_value = target_clean.strip()
                chat_id_int = int(test_value)
                is_chat_id = True
            except (ValueError, AttributeError):
                pass
            
            # 如果不是 Chat ID，处理用户名格式
            if not is_chat_id:
                if not target_clean.startswith('@'):
                    if not target_clean.startswith('http'):
                        target_clean = '@' + target_clean.lstrip('@')
            
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
            
            # 构建按钮
            buttons = []
            msg_link = None
            if source_chat_id and msg_id:
                try:
                    chat_username = getattr(chat, 'username', None) if chat else None
                    
                    if chat_username:
                        msg_link = f"https://t.me/{chat_username}/{msg_id}"
                    else:
                        chat_id_str = str(source_chat_id)
                        if chat_id_str.startswith('-100'):
                            channel_id = chat_id_str[4:]
                            if channel_id.isdigit():
                                msg_link = f"https://t.me/c/{channel_id}/{msg_id}"
                        elif chat_id_str.startswith('-'):
                            msg_link = f"tg://openmessage?chat_id={source_chat_id}&message_id={msg_id}"
                        else:
                            msg_link = f"https://t.me/c/{source_chat_id}/{msg_id}"
                except:
                    pass
            
            if not msg_link and source_chat_id and msg_id:
                msg_link = f"tg://openmessage?chat_id={source_chat_id}&message_id={msg_id}"
            
            button_row = []
            if msg_link and (msg_link.startswith('https://') or msg_link.startswith('tg://')):
                button_row.append(Button.url('👁️ 查看消息', msg_link))
            
            if button_row:
                buttons.append(button_row)
            
            if sender_id:
                buttons.append([Button.inline('🚫 屏蔽该用户', data=f'block_user:{sender_id}')])
            
            # 发送消息（使用send_queue，确保并发）
            try:
                if is_chat_id:
                    target_entity = chat_id_int
                else:
                    target_entity = target_clean
                
                _ensure_send_workers(bot_client)
                
                _send_queue.put_nowait((
                    bot_client,
                    target_entity,
                    message_text,
                    buttons if buttons else None,
                    'markdown'
                ))
                
                delivered = 'success'
                error = None
            except Exception as send_error:
                delivered = 'error'
                error = str(send_error)[:200]
        except ValueError as ve:
            delivered = 'error'
            error = str(ve)
        except Exception as e:
            delivered = 'error'
            error = f"Failed to send: {str(e)[:200]}"

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


async def send_alert(bot_client, account, event, matched_keyword: str, control_bot_id=None):
    """
    兼容性函数：保持向后兼容，内部调用 quick_enqueue_alert
    """
    quick_enqueue_alert(bot_client, account, event, matched_keyword, control_bot_id)
    """发送提醒：完全异步，不阻塞，立即入队"""
    from datetime import datetime
    
    # 立即获取基本信息，不等待
    text = event.message.message or ''
    msg_id = getattr(event.message, 'id', None)
    chat_id = getattr(event, 'chat_id', None)
    
    # 异步获取 sender 和 chat，不阻塞主流程
    async def _get_info():
        try:
            sender, chat = await asyncio.wait_for(
                asyncio.gather(
                    event.get_sender(),
                    event.get_chat(),
                    return_exceptions=True
                ),
                timeout=0.2  # 0.2秒超时，快速失败
            )
            
            # 处理异常
            if isinstance(sender, Exception):
                sender = None
            if isinstance(chat, Exception):
                chat = None
            
            # 快速检查：如果消息来自控制机器人，跳过发送
            if sender:
                sender_id = getattr(sender, 'id', None)
                is_bot = getattr(sender, 'bot', False)
                if is_bot and control_bot_id and sender_id == control_bot_id:
                    return None, None, None  # 跳过发送
            
            return sender, chat, None
        except asyncio.TimeoutError:
            return None, None, None
        except Exception:
            return None, None, None
    
    # 在后台获取信息，不阻塞
    info_task = asyncio.create_task(_get_info())
    
    # 立即构建基本消息内容，不等待信息获取
    account_id = account['id']
    account_username = account.get('username')
    if account_username:
        account_display = f"@{account_username}"
    else:
        account_display = account.get('phone') or f"#{account_id}"
    
    # 等待信息获取完成（但使用超时）
    try:
        sender, chat, skip = await asyncio.wait_for(info_task, timeout=0.1)
        if skip is not None:  # 跳过发送
            return
    except asyncio.TimeoutError:
        # 超时后使用默认值继续
        sender = None
        chat = None
    
    # 安全获取信息，处理 None 情况（快速处理，不阻塞）
    sender_name = 'Unknown'
    sender_username = None
    sender_id = None
    if sender:
        sender_name = f"{getattr(sender,'first_name', '') or ''} {getattr(sender,'last_name','') or ''}".strip() or 'Unknown'
        sender_username = getattr(sender, 'username', None)
        sender_id = getattr(sender, 'id', None)
    
    sender_username_display = f"@{sender_username}" if sender_username else '无'
    source_title = (getattr(chat, 'title', '') or getattr(chat, 'username','') or 'Unknown') if chat else 'Unknown'
    source_chat_id = getattr(chat, 'id', None) if chat else chat_id  # 使用 chat_id 作为备选

    # 使用全局转发目标
    target = settings_service.get_target_chat()
    if not target or not target.strip():
        error_timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
        print(f"[发送提醒] [{error_timestamp}] ❌ 转发目标未配置")
        delivered = 'error'
        error = 'Target chat not configured'
    else:
        try:
            # 处理转发目标格式
            target_clean = target.strip()
            
            # 检查是否是邀请链接（机器人无法解析邀请链接）
            if target_clean.startswith('https://t.me/+') or target_clean.startswith('https://t.me/joinchat/') or target_clean.startswith('t.me/+') or target_clean.startswith('t.me/joinchat/'):
                raise ValueError('转发目标不能是邀请链接。机器人无法解析邀请链接。请使用：\n• 群组/频道用户名（如 @groupname）\n• Chat ID（如 -1001234567890）\n• 机器人已加入的公开群组/频道')
            
            # 检查是否是 Chat ID（数字格式，包括负数）
            is_chat_id = False
            chat_id_int = None
            try:
                # 尝试解析为整数（支持负数）
                # 移除可能的空格和特殊字符
                test_value = target_clean.strip()
                chat_id_int = int(test_value)
                is_chat_id = True
                chat_id_timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
                print(f"[发送提醒] [{chat_id_timestamp}] 检测到 Chat ID 格式: {chat_id_int}")
            except (ValueError, AttributeError):
                pass
            
            # 如果不是 Chat ID，处理用户名格式
            if not is_chat_id:
                # 如果目标不是以 @ 开头且不是数字（chat_id），尝试添加 @
                if not target_clean.startswith('@'):
                    # 可能是用户名但没有 @，尝试添加
                    if not target_clean.startswith('http'):
                        target_clean = '@' + target_clean.lstrip('@')
            
            # 构建消息内容（使用Markdown富文本格式，美观协调）
            account_id = account['id']
            account_username = account.get('username')
            if account_username:
                account_display = f"@{account_username}"
            else:
                account_display = account.get('phone') or f"#{account_id}"
            
            # 转义Markdown特殊字符，防止格式错误（只转义必要的字符）
            def escape_md(text):
                if not text:
                    return ''
                # 只转义在Markdown中有特殊意义的字符
                text = str(text)
                # 转义反引号、星号、下划线、方括号等
                text = text.replace('\\', '\\\\')  # 先转义反斜杠
                text = text.replace('*', '\\*')
                text = text.replace('_', '\\_')
                text = text.replace('[', '\\[')
                text = text.replace(']', '\\]')
                text = text.replace('(', '\\(')
                text = text.replace(')', '\\)')
                text = text.replace('`', '\\`')
                return text
            
            # 构建消息格式（去掉分隔线，直接显示内容）
            message_text = (
                f"🔔 **关键词提醒**\n\n"
                f"📱 **监听账号：** `{escape_md(account_display)}`\n"
                f"🔑 **关键字：** `{escape_md(matched_keyword)}`\n"
                f"👤 **发送者：** {escape_md(sender_name)}\n"
                f"📝 **用户名：** {escape_md(sender_username_display)}\n"
                f"💬 **来源群组：** `{escape_md(source_title)}`\n"
                f"📄 **消息内容：** {escape_md(text)}"
            )
            
            # 构建按钮（快速生成，不阻塞）
            buttons = []
            msg_link = None
            if source_chat_id and msg_id:
                try:
                    # 快速生成链接，不等待
                    chat_username = getattr(chat, 'username', None) if chat else None
                    
                    if chat_username:
                        msg_link = f"https://t.me/{chat_username}/{msg_id}"
                    else:
                        chat_id_str = str(source_chat_id)
                        if chat_id_str.startswith('-100'):
                            channel_id = chat_id_str[4:]
                            if channel_id.isdigit():
                                msg_link = f"https://t.me/c/{channel_id}/{msg_id}"
                        elif chat_id_str.startswith('-'):
                            msg_link = f"tg://openmessage?chat_id={source_chat_id}&message_id={msg_id}"
                        else:
                            msg_link = f"https://t.me/c/{source_chat_id}/{msg_id}"
                except:
                    pass
            
            # 如果还没有链接，使用备选
            if not msg_link and source_chat_id and msg_id:
                msg_link = f"tg://openmessage?chat_id={source_chat_id}&message_id={msg_id}"
            
            # 构建按钮行
            button_row = []
            if msg_link and (msg_link.startswith('https://') or msg_link.startswith('tg://')):
                button_row.append(Button.url('👁️ 查看消息', msg_link))
            
            if button_row:
                buttons.append(button_row)
            
            # 屏蔽用户按钮（单独一行）
            if sender_id:
                buttons.append([Button.inline('🚫 屏蔽该用户', data=f'block_user:{sender_id}')])
            
            # 立即入队，不等待
            try:
                if is_chat_id:
                    target_entity = chat_id_int
                else:
                    target_entity = target_clean
                
                # 确保发送工作协程已启动
                _ensure_send_workers(bot_client)
                
                # 立即入队，不等待
                _send_queue.put_nowait((
                    bot_client,
                    target_entity,
                    message_text,
                    buttons if buttons else None,
                    'markdown'
                ))
                
                delivered = 'success'
                error = None
            except Exception as send_error:
                # 错误由工作协程处理，这里只记录
                delivered = 'error'
                error = str(send_error)[:200]
        except ValueError as ve:
            delivered = 'error'
            error = str(ve)
        except Exception as e:
            delivered = 'error'
            error = f"Failed to send: {str(e)[:200]}"

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

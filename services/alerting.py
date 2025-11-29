from telethon import events
from telethon.tl.types import PeerUser, PeerChat, PeerChannel
from telethon.tl.custom import Button
from storage import dao_alerts
from services import settings_service

async def send_alert(bot_client, account, event, matched_keyword: str):
    from datetime import datetime
    print(f"[发送提醒] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 开始构建提醒消息...")
    
    sender = await event.get_sender()
    chat = await event.get_chat()
    sender_name = f"{getattr(sender,'first_name', '') or ''} {getattr(sender,'last_name','') or ''}".strip() or 'Unknown'
    sender_username = getattr(sender, 'username', None)
    sender_username_display = f"@{sender_username}" if sender_username else '无'
    source_title = getattr(chat, 'title', '') or getattr(chat, 'username','') or 'Unknown'
    text = event.message.message or ''
    source_chat_id = getattr(chat, 'id', None)
    sender_id = getattr(sender, 'id', None)
    
    print(f"[发送提醒] 发送者: {sender_name} ({sender_username_display})")
    print(f"[发送提醒] 来源群组: {source_title} (ID: {source_chat_id})")
    print(f"[发送提醒] 消息内容: {text[:100]}...")

    # 使用全局转发目标
    target = settings_service.get_target_chat()
    print(f"[发送提醒] 转发目标: {target}")
    if not target or not target.strip():
        print(f"[发送提醒] ❌ 转发目标未配置")
        delivered = 'error'
        error = 'Target chat not configured'
    else:
        try:
            # 处理转发目标格式
            target_clean = target.strip()
            # 如果目标不是以 @ 开头且不是数字（chat_id），尝试添加 @
            if not target_clean.startswith('@') and not target_clean.lstrip('-').isdigit():
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
            
            # 构建按钮（添加emoji和合适的按钮）
            buttons = []
            # 尝试构建消息链接
            msg_link = None
            if source_chat_id and event.message.id:
                try:
                    # 优先尝试使用群组的 username（公开群组/频道）
                    chat_username = getattr(chat, 'username', None)
                    if chat_username:
                        # 公开群组/频道，使用 username 格式
                        msg_link = f"https://t.me/{chat_username}/{event.message.id}"
                    elif str(source_chat_id).startswith('-100'):
                        # 私有超级群组/频道，chat_id 格式为 -100xxxxxxxxxx
                        # 提取频道ID（去掉 -100 前缀）
                        channel_id = str(source_chat_id)[4:]
                        msg_link = f"https://t.me/c/{channel_id}/{event.message.id}"
                    else:
                        # 普通群组，使用 tg:// 协议（可能不太可靠，但作为备选）
                        msg_link = f"tg://openmessage?chat_id={source_chat_id}&message_id={event.message.id}"
                except Exception:
                    pass
            
            # 构建按钮行 - 只添加"查看消息"按钮
            button_row = []
            if msg_link:
                button_row.append(Button.url('👁️ 查看消息', msg_link))
            if button_row:
                buttons.append(button_row)
            
            # 屏蔽用户按钮（单独一行）
            if sender_id:
                buttons.append([Button.inline('🚫 屏蔽该用户', data=f'block_user:{sender_id}')])
            
            # 使用机器人客户端发送消息
            # 使用Markdown解析模式
            print(f"[发送提醒] 准备发送到: {target_clean}")
            print(f"[发送提醒] 消息长度: {len(message_text)} 字符")
            await bot_client.send_message(
                target_clean, 
                message_text, 
                parse_mode='markdown',
                buttons=buttons if buttons else None
            )
            print(f"[发送提醒] ✅ 消息发送成功到 {target_clean}")
            delivered = 'success'
            error = None
        except Exception as e:
            print(f"[发送提醒] ❌ 发送失败: {str(e)}")
            import traceback
            traceback.print_exc()
            delivered = 'error'
            error = f"Failed to send to {target}: {str(e)}"

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

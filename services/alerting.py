from telethon import events
from telethon.tl.types import PeerUser, PeerChat, PeerChannel
from telethon.tl.custom import Button
from storage import dao_alerts
from services import settings_service

async def send_alert(bot_client, account, event, matched_keyword: str):
    from datetime import datetime
    start_time = datetime.now()
    timestamp = start_time.strftime('%H:%M:%S.%f')[:-3]  # 毫秒精度
    print(f"[发送提醒] [{timestamp}] 开始构建提醒消息...")
    
    # 极致优化：并发获取 sender 和 chat，不串行等待
    sender_task = asyncio.create_task(event.get_sender())
    chat_task = asyncio.create_task(event.get_chat())
    sender = await sender_task
    chat = await chat_task
    sender_name = f"{getattr(sender,'first_name', '') or ''} {getattr(sender,'last_name','') or ''}".strip() or 'Unknown'
    sender_username = getattr(sender, 'username', None)
    sender_username_display = f"@{sender_username}" if sender_username else '无'
    source_title = getattr(chat, 'title', '') or getattr(chat, 'username','') or 'Unknown'
    text = event.message.message or ''
    source_chat_id = getattr(chat, 'id', None)
    sender_id = getattr(sender, 'id', None)
    
    # 尝试获取更详细的 chat 信息（用于生成链接）
    chat_entity = None
    try:
        # 尝试通过 client 获取实体信息（可能包含更多信息）
        if hasattr(event, 'client') and event.client:
            try:
                chat_entity = await event.client.get_entity(chat)
            except:
                pass
    except:
        pass
    
    info_timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
    print(f"[发送提醒] [{info_timestamp}] 发送者: {sender_name} ({sender_username_display})")
    print(f"[发送提醒] [{info_timestamp}] 来源群组: {source_title} (ID: {source_chat_id})")
    print(f"[发送提醒] [{info_timestamp}] 消息内容: {text[:100]}...")

    # 使用全局转发目标
    target = settings_service.get_target_chat()
    print(f"[发送提醒] [{info_timestamp}] 转发目标: {target}")
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
            
            # 构建按钮（添加emoji和合适的按钮）
            buttons = []
            # 尝试构建消息链接（优化：支持所有类型的群组/频道，确保链接可点击）
            msg_link = None
            if source_chat_id and event.message.id:
                try:
                    # 优先尝试使用群组的 username（公开群组/频道）
                    chat_username = getattr(chat, 'username', None)
                    # 如果从 chat 对象获取不到，尝试从 chat_entity 获取
                    if not chat_username and chat_entity:
                        chat_username = getattr(chat_entity, 'username', None)
                    
                    if chat_username:
                        # 公开群组/频道，使用 username 格式（最可靠，所有客户端都支持）
                        msg_link = f"https://t.me/{chat_username}/{event.message.id}"
                        link_timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
                        print(f"[发送提醒] [{link_timestamp}] ✅ 生成公开链接: {msg_link} (username: {chat_username})")
                    else:
                        # 私有群组/频道，需要特殊处理
                        chat_id_str = str(source_chat_id)
                        
                        # 检查是否是超级群组/频道（-100 开头）
                        if chat_id_str.startswith('-100'):
                            # 私有超级群组/频道
                            # Telegram 的私有频道链接格式：https://t.me/c/{channel_id}/{message_id}
                            # channel_id 需要是正数，从 -100xxxxxxxxxx 中提取
                            # 注意：需要去掉负号和 -100 前缀
                            channel_id = chat_id_str[4:]  # 去掉 "-100" 前缀
                            # 确保是有效的数字
                            if channel_id.isdigit():
                                # 尝试使用 https:// 链接（如果用户已加入频道，这个链接可以工作）
                                msg_link = f"https://t.me/c/{channel_id}/{event.message.id}"
                                link_timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
                                print(f"[发送提醒] [{link_timestamp}] ✅ 生成私有频道链接: {msg_link} (原始 Chat ID: {source_chat_id}, 频道 ID: {channel_id})")
                            else:
                                link_timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
                                print(f"[发送提醒] [{link_timestamp}] ⚠️ 无法生成私有频道链接: channel_id={channel_id} 格式无效 (原始: {source_chat_id})")
                        elif chat_id_str.startswith('-'):
                            # 普通私有群组（负数但不是 -100 开头）
                            # 对于普通群组，Telegram 不支持 https:// 链接，但可以尝试使用 tg:// 协议
                            # 虽然 tg:// 协议在某些客户端可能不工作，但总比没有按钮好
                            try:
                                # 尝试使用 tg:// 协议（至少提供一个可点击的按钮）
                                # 格式：tg://openmessage?chat_id={chat_id}&message_id={message_id}
                                # 注意：chat_id 需要是负数格式
                                msg_link = f"tg://openmessage?chat_id={source_chat_id}&message_id={event.message.id}"
                                print(f"[发送提醒] ⚠️ 普通群组 (ID: {source_chat_id})，生成 tg:// 协议链接: {msg_link} (可能在某些客户端不可用)")
                            except Exception as e:
                                print(f"[发送提醒] ⚠️ 生成普通群组链接失败: {e}")
                        else:
                            # 正数 chat_id（可能是频道或特殊类型）
                            # 对于正数 Chat ID，优先尝试使用 https:// 链接（作为频道处理）
                            try:
                                # 尝试 1: 直接使用正数作为频道 ID（https://t.me/c/{chat_id}/{message_id}）
                                # 这适用于某些类型的频道
                                msg_link = f"https://t.me/c/{source_chat_id}/{event.message.id}"
                                link_timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
                                print(f"[发送提醒] [{link_timestamp}] ✅ 正数 Chat ID: {source_chat_id}，尝试生成 https:// 链接: {msg_link}")
                                
                                # 尝试 2: 如果上面的格式不工作，也可以尝试转换为 -100 格式
                                # 某些情况下，正数 Chat ID 可能需要转换为 -100{chat_id} 格式
                                # 但先使用直接格式，如果不行再尝试转换
                                
                            except Exception as e:
                                print(f"[发送提醒] ⚠️ 生成正数 Chat ID https:// 链接失败: {e}")
                                # 如果 https:// 链接生成失败，回退到 tg:// 协议
                                try:
                                    msg_link = f"tg://openmessage?chat_id={source_chat_id}&message_id={event.message.id}"
                                    print(f"[发送提醒] ⚠️ 回退到 tg:// 协议链接: {msg_link}")
                                except Exception as e2:
                                    print(f"[发送提醒] ⚠️ 生成 tg:// 协议链接也失败: {e2}")
                except Exception as e:
                    print(f"[发送提醒] ❌ 生成消息链接时出错: {e}")
                    import traceback
                    traceback.print_exc()
            
            # 构建按钮行 - 只添加"查看消息"按钮
            button_row = []
            if msg_link:
                # 验证链接格式是否正确
                if msg_link.startswith('https://') or msg_link.startswith('tg://'):
                    button_row.append(Button.url('👁️ 查看消息', msg_link))
                    button_timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
                    print(f"[发送提醒] [{button_timestamp}] ✅ 已添加'查看消息'按钮，链接: {msg_link}")
                else:
                    print(f"[发送提醒] ⚠️ 链接格式无效: {msg_link}")
                    # 尝试生成备选链接
                    msg_id = getattr(event.message, 'id', None) if hasattr(event, 'message') and event.message else None
                    if source_chat_id and msg_id:
                        fallback_link = f"tg://openmessage?chat_id={source_chat_id}&message_id={msg_id}"
                        button_row.append(Button.url('👁️ 查看消息', fallback_link))
                        print(f"[发送提醒] ✅ 使用备选链接: {fallback_link}")
            else:
                # 如果无法生成链接，尝试使用最基本的 tg:// 链接作为备选
                msg_id = None
                if hasattr(event, 'message') and event.message:
                    msg_id = getattr(event.message, 'id', None)
                
                if source_chat_id and msg_id:
                    try:
                        # 尝试生成备选链接（无论 Chat ID 是正数还是负数）
                        fallback_link = f"tg://openmessage?chat_id={source_chat_id}&message_id={msg_id}"
                        button_row.append(Button.url('👁️ 查看消息', fallback_link))
                        print(f"[发送提醒] ✅ 使用备选 tg:// 链接: {fallback_link} (Chat ID: {source_chat_id}, Message ID: {msg_id})")
                    except Exception as e:
                        print(f"[发送提醒] ❌ 生成备选链接失败: {e}")
                        print(f"[发送提醒] ⚠️ 无法生成消息链接 (Chat ID: {source_chat_id}, Message ID: {msg_id})")
                else:
                    # 如果连基本信息都没有，记录详细日志
                    print(f"[发送提醒] ⚠️ 无法生成消息链接 - 缺少必要信息 (Chat ID: {source_chat_id}, Message ID: {msg_id})")
            
            if button_row:
                buttons.append(button_row)
            
            # 屏蔽用户按钮（单独一行）
            if sender_id:
                buttons.append([Button.inline('🚫 屏蔽该用户', data=f'block_user:{sender_id}')])
            
            # 使用机器人客户端发送消息
            # 使用Markdown解析模式
            send_prep_timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
            print(f"[发送提醒] [{send_prep_timestamp}] 准备发送到: {target_clean}")
            print(f"[发送提醒] [{send_prep_timestamp}] 消息长度: {len(message_text)} 字符")
            
            try:
                # 如果是 Chat ID，直接使用整数；否则使用字符串（用户名）
                if is_chat_id:
                    target_entity = chat_id_int
                    print(f"[发送提醒] [{send_prep_timestamp}] 使用 Chat ID 发送: {target_entity}")
                else:
                    target_entity = target_clean
                    print(f"[发送提醒] [{send_prep_timestamp}] 使用用户名发送: {target_entity}")
                
                send_start_time = datetime.now()
                send_start_timestamp = send_start_time.strftime('%H:%M:%S.%f')[:-3]
                print(f"[发送提醒] [{send_start_timestamp}] 开始发送消息...")
                
                await bot_client.send_message(
                    target_entity, 
                    message_text, 
                    parse_mode='markdown',
                    buttons=buttons if buttons else None
                )
                
                send_end_time = datetime.now()
                send_end_timestamp = send_end_time.strftime('%H:%M:%S.%f')[:-3]
                send_duration = (send_end_time - send_start_time).total_seconds()
                print(f"[发送提醒] [{send_end_timestamp}] ✅ 消息发送成功到 {target_entity} (耗时: {send_duration:.3f}秒)")
                delivered = 'success'
                error = None
            except Exception as send_error:
                error_str = str(send_error)
                error_type = type(send_error).__name__
                
                # 处理常见的错误类型
                if 'BotMethodInvalidError' in error_type or 'CheckChatInviteRequest' in error_str:
                    # 机器人无法解析邀请链接或访问某些实体
                    if 'joinchat' in target_clean.lower() or '/+' in target_clean.lower():
                        error_msg = '转发目标不能是邀请链接。请使用群组/频道用户名（@groupname）或 Chat ID'
                    else:
                        if is_chat_id:
                            error_msg = '机器人无法访问该 Chat ID。请确保：\n• 机器人已加入该群组/频道\n• Chat ID 格式正确（如 -1001234567890）\n• 机器人有发送消息权限'
                        else:
                            error_msg = '机器人无法访问该目标。请确保：\n• 目标是一个公开的群组/频道（@username）\n• 或者机器人已加入该群组/频道\n• 或者使用 Chat ID（如 -1001234567890）'
                elif 'CHAT_NOT_FOUND' in error_str or 'PEER_ID_INVALID' in error_str:
                    if is_chat_id:
                        error_msg = f'无法找到 Chat ID {chat_id_int}。请确保：\n• 机器人已加入该群组/频道\n• Chat ID 正确（可以通过"诊断群组 #账号ID"查看）'
                    else:
                        error_msg = '目标群组/频道不存在或无法访问。请检查转发目标设置'
                elif 'USERNAME_INVALID' in error_str:
                    error_msg = '用户名格式无效。请检查转发目标设置'
                elif 'CHAT_WRITE_FORBIDDEN' in error_str or 'FORBIDDEN' in error_str:
                    error_msg = '机器人没有权限在该群组/频道发送消息。请确保机器人是管理员或有发送消息权限'
                elif 'CHANNEL_PRIVATE' in error_str:
                    if is_chat_id:
                        error_msg = f'Chat ID {chat_id_int} 对应的群组/频道是私有的，且机器人未加入。请确保机器人已加入该群组/频道'
                    else:
                        error_msg = '该群组/频道是私有的，且机器人未加入。请使用 Chat ID 或确保机器人已加入'
                else:
                    error_msg = f'发送失败：{error_str[:200]}'
                
                print(f"[发送提醒] ❌ 发送失败 ({error_type}): {error_msg}")
                import traceback
                traceback.print_exc()
                delivered = 'error'
                error = error_msg
        except ValueError as ve:
            # 处理我们主动抛出的错误（如邀请链接检测）
            print(f"[发送提醒] ❌ 配置错误: {str(ve)}")
            delivered = 'error'
            error = str(ve)
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

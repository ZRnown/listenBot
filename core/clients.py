import asyncio
import os
import time
import app.config as cfg
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors.rpcerrorlist import FloodWaitError
from storage import dao_accounts
from services import sessions as sess_service
from services import settings_service
from core.handlers import on_new_message


class ClientManager:
    def __init__(self, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.bot = None
        self.bot_token = os.getenv('BOT_TOKEN')
        self.api_id = int(os.getenv('API_ID', '0') or 0) or None
        self.api_hash = os.getenv('API_HASH')
        if not (self.bot_token and self.api_id and self.api_hash):
            raise RuntimeError('BOT_TOKEN, API_ID, API_HASH are required in environment')
        self.account_clients = {}  # account_id -> TelegramClient
        self._handlers_setup = False  # 标记处理器是否已设置
        self.bot_id = None  # 控制机器人的 ID（用于过滤自己的消息）

    async def start_control_bot(self):
        # 如果 bot 已存在，先断开连接
        if self.bot:
            try:
                await self.bot.disconnect()
            except Exception:
                pass
            self.bot = None
        
        # 使用内存 session（不保存到文件），仅使用 BOT_TOKEN 登录
        # Telethon 要求必须有 session 参数，但使用 StringSession() 创建空的内存 session
        # 这样不会创建任何 session 文件，完全依赖 bot_token
        from telethon.sessions import StringSession
        memory_session = StringSession()  # 空的内存 session，不保存到文件
        self.bot = TelegramClient(memory_session, self.api_id, self.api_hash)
        await self.bot.start(bot_token=self.bot_token)
        
        # 获取控制机器人的 ID
        try:
            bot_me = await self.bot.get_me()
            self.bot_id = bot_me.id
            print(f"[启动] 机器人已使用 BOT_TOKEN 登录（完全使用 token，无 session 文件）")
            print(f"[启动] 控制机器人 ID: {self.bot_id}")
        except Exception as e:
            print(f"[启动] ⚠️ 无法获取控制机器人 ID: {str(e)}")
            self.bot_id = None
        
        self._handlers_setup = False  # 重置标志
        return self.bot

    async def stop(self):
        for c in list(self.account_clients.values()):
            await c.disconnect()
        self.account_clients.clear()
        if self.bot:
            await self.bot.disconnect()

    async def add_account_from_session_file(self, file_path: str):
        session_name = os.path.splitext(file_path)[0]
        client = TelegramClient(session_name, self.api_id, self.api_hash)
        
        # 快速验证 session（不等待完全连接）
        try:
            await client.connect()
            if not await client.is_user_authorized():
                await client.disconnect()
                raise RuntimeError('Session not authorized or requires login')
        except Exception as e:
            try:
                await client.disconnect()
            except:
                pass
            raise RuntimeError(f'Session validation failed: {str(e)}')
        
        # 获取用户信息（快速操作）
        try:
            me = await client.get_me()
        except Exception as e:
            await client.disconnect()
            raise RuntimeError(f'Failed to get user info: {str(e)}')
        
        phone = getattr(me, 'phone', None)
        username = getattr(me, 'username', None)
        nickname = (getattr(me, 'first_name', '') or '') + ' ' + (getattr(me, 'last_name', '') or '')
        
        existing = dao_accounts.find_by_phone_or_username(phone, username)
        if existing:
            account_id = existing['id']
            conn = cfg.pool.connection()
            cur = conn.cursor()
            try:
                cur.execute("UPDATE accounts SET session_path=%s, status='active' WHERE id=%s", (file_path, account_id))
                conn.commit()
            finally:
                cur.close()
                conn.close()
            if account_id in self.account_clients:
                await self.account_clients[account_id].disconnect()
            self._register_handlers_for_account(client, account_id)
            self.account_clients[account_id] = client
            
            # 异步启动客户端（不阻塞返回）
            asyncio.create_task(self._ensure_client_connected(client, account_id))
            
            return {
                'id': account_id,
                'phone': phone,
                'username': f"@{username}" if username else None,
                'nickname': nickname.strip(),
                'existing': True
            }
        else:
            account_id = dao_accounts.create(phone, nickname.strip(), username, file_path, status='active')
            # 复制已有账号的关键词到新账号
            self._copy_keywords_to_new_account(account_id)
        
        # 注册处理器并保存客户端
        self._register_handlers_for_account(client, account_id)
        self.account_clients[account_id] = client
        
        # 异步启动账号客户端（不阻塞返回）
        account_row = dao_accounts.get(account_id)
        if account_row:
            asyncio.create_task(self.start_account_client(account_row))
        else:
            # 如果获取不到账号信息，至少确保客户端连接
            asyncio.create_task(self._ensure_client_connected(client, account_id))
        
        return {
            'id': account_id,
            'phone': phone,
            'username': f"@{username}" if username else None,
            'nickname': nickname.strip(),
            'existing': False
        }

    async def add_account_from_string_session(self, session_str: str):
        try:
            sess = StringSession(session_str)
        except Exception:
            raise RuntimeError('无效的 StringSession 文本，请检查后重新发送')
        client = TelegramClient(sess, self.api_id, self.api_hash)
        
        # 快速验证 session（不等待完全连接）
        try:
            await client.connect()
            if not await client.is_user_authorized():
                await client.disconnect()
                raise RuntimeError('Session 未授权或需要登录')
        except Exception as e:
            try:
                await client.disconnect()
            except:
                pass
            if isinstance(e, RuntimeError):
                raise
            raise RuntimeError(f'Session validation failed: {str(e)}')
        
        # 获取用户信息（快速操作）
        try:
            me = await client.get_me()
        except Exception as e:
            await client.disconnect()
            raise RuntimeError(f'Failed to get user info: {str(e)}')
        
        phone = getattr(me, 'phone', None)
        username = getattr(me, 'username', None)
        nickname = (getattr(me, 'first_name', '') or '') + ' ' + (getattr(me, 'last_name', '') or '')
        
        existing = dao_accounts.find_by_phone_or_username(phone, username)
        if existing:
            account_id = existing['id']
            conn = cfg.pool.connection()
            cur = conn.cursor()
            try:
                cur.execute("UPDATE accounts SET session_path=%s, status='active' WHERE id=%s", (session_str, account_id))
                conn.commit()
            finally:
                cur.close()
                conn.close()
            if account_id in self.account_clients:
                await self.account_clients[account_id].disconnect()
            self._register_handlers_for_account(client, account_id)
            self.account_clients[account_id] = client
            
            # 异步启动客户端（不阻塞返回）
            asyncio.create_task(self._ensure_client_connected(client, account_id))
            
            return {
                'id': account_id,
                'phone': phone,
                'username': f"@{username}" if username else None,
                'nickname': nickname.strip(),
                'existing': True
            }
        else:
            account_id = dao_accounts.create(phone, nickname.strip(), username, session_str, status='active')
            # 复制已有账号的关键词到新账号
            self._copy_keywords_to_new_account(account_id)
        
        # 注册处理器并保存客户端
        self._register_handlers_for_account(client, account_id)
        self.account_clients[account_id] = client
        
        # 异步启动账号客户端（不阻塞返回）
        account_row = dao_accounts.get(account_id)
        if account_row:
            asyncio.create_task(self.start_account_client(account_row))
        else:
            # 如果获取不到账号信息，至少确保客户端连接
            asyncio.create_task(self._ensure_client_connected(client, account_id))
        
        return {
            'id': account_id,
            'phone': phone,
            'username': f"@{username}" if username else None,
            'nickname': nickname.strip(),
            'existing': False
        }

    def _copy_keywords_to_new_account(self, account_id: int):
        """复制全局点击关键词到新账号（如果该账号是点击账号）"""
        from services import settings_service
        settings_service.apply_global_click_keywords_to_account(account_id)
    
    async def _ensure_client_connected(self, client: TelegramClient, account_id: int):
        """确保客户端在后台完全连接（异步执行，不阻塞）"""
        try:
            if not client.is_connected():
                await client.connect()
            # 确保客户端已启动
            if not await client.is_user_authorized():
                print(f"[客户端连接] 账号 #{account_id} 未授权，跳过启动")
                return
            print(f"[客户端连接] 账号 #{account_id} 客户端已连接")
        except Exception as e:
            print(f"[客户端连接] 账号 #{account_id} 连接失败: {e}")

    def _register_handlers_for_account(self, client: TelegramClient, account_id: int, group_list: list = None):
        """为账号注册事件处理器（支持多账号并发）"""
        if group_list:
            group_ids_set = {g['id'] for g in group_list}
            client._monitored_group_ids = group_ids_set
            print(f"[事件注册] 账号 #{account_id} 注册事件处理器，监控 {len(group_ids_set)} 个群组")
        else:
            client._monitored_group_ids = None
            print(f"[事件注册] 账号 #{account_id} 注册事件处理器，监控所有群组（无限制）")
        
        @client.on(events.NewMessage(incoming=True))
        async def handle_new_message(event):
            # 如果指定了群组列表，只处理这些群组的消息
            if group_list and hasattr(client, '_monitored_group_ids') and client._monitored_group_ids is not None:
                if event.chat_id not in client._monitored_group_ids:
                    return
            # 如果没有指定群组列表，处理所有群组消息
            await self._process_message(event, account_id, "NewMessage")
        
        @client.on(events.MessageEdited(incoming=True))
        async def handle_message_edited(event):
            # 如果指定了群组列表，只处理这些群组的消息
            if group_list and hasattr(client, '_monitored_group_ids') and client._monitored_group_ids is not None:
                if event.chat_id not in client._monitored_group_ids:
                    return
            # 如果没有指定群组列表，处理所有群组消息
            await self._process_message(event, account_id, "MessageEdited")
    
    async def _process_message(self, event, account_id: int, handler_name: str):
        """处理收到的消息（极致优化：立即处理，不阻塞）"""
        try:
            # 快速过滤：只处理群组消息
            if event.is_private or not event.is_group:
                return
            
            # 添加调试日志（仅在收到消息时打印，确认事件被触发）
            msg_id = getattr(event.message, 'id', None)
            chat_id = getattr(event, 'chat_id', None)
            text_preview = (getattr(event.message, 'raw_text', '') or event.message.message or '')[:50]
            print(f"[事件触发] 账号 #{account_id} 收到消息 (Handler: {handler_name}, 消息ID: {msg_id}, Chat ID: {chat_id}, 文本预览: {text_preview}...)")
            
            account = dao_accounts.get(account_id)
            if account:
                # 极致优化：直接调用，不等待完成，让关键词检测和推送立即进行
                # 传递控制机器人的 ID，用于过滤自己的消息
                # 注意：on_new_message 内部已经使用 create_task 来发送提醒，所以这里直接 await 不会阻塞
                await on_new_message(event, account, self.bot, self.bot_id)
            else:
                print(f"[处理消息] ⚠️ 账号 #{account_id} 不存在于数据库中")
        except (GeneratorExit, asyncio.CancelledError):
            # 优雅处理协程取消
            pass
        except Exception as e:
            print(f"[处理消息] ❌ 账号 #{account_id} 错误: {str(e)}")
            import traceback
            traceback.print_exc()

    async def start_account_client(self, account_row):
        account_id = account_row['id']
        session_path = account_row['session_path']
        if session_path and os.path.exists(session_path):
            session_name = os.path.splitext(session_path)[0]
            client = TelegramClient(session_name, self.api_id, self.api_hash)
        else:
            try:
                sess = StringSession(session_path)
            except Exception as e:
                raise RuntimeError(f'存储的会话字符串无效，无法恢复该账号: {str(e)}')
            client = TelegramClient(sess, self.api_id, self.api_hash)
        
        await client.start(phone=lambda: None, password=lambda: None, code_callback=lambda: None)
        group_list = await self._list_account_groups(client, account_id)
        await self._sync_all_groups(client, account_id, group_list)
        self._register_handlers_for_account(client, account_id, group_list)
        self.account_clients[account_id] = client
        await client.catch_up()
        
        # ❌ 移除主动轮询：Telethon 的 events.NewMessage 已经通过 TCP 长连接实时推送消息
        # 主动轮询会导致：
        # 1. 网络延迟（RTT）：每次 get_messages 需要等待网络往返
        # 2. FloodWait 限流：频繁请求会触发 Telegram 的限流机制，导致账号被暂停
        # 3. 资源浪费：占用大量 CPU 和网络带宽
        # 
        # ✅ 正确方案：完全依赖 events.NewMessage 事件推送
        # Telegram 服务端会通过 TCP 长连接主动推送新消息，响应速度远快于主动轮询
        # 且不会触发限流，不会漏消息
        print(f"[启动] 账号 #{account_id} 已注册事件处理器，完全依赖 TCP 推送（无主动轮询）")
    
    async def _list_account_groups(self, client: TelegramClient, account_id: int):
        """列出账号加入的所有群组"""
        try:
            groups = []
            async for dialog in client.iter_dialogs():
                if not dialog.is_user:
                    chat = dialog.entity
                    chat_id = chat.id
                    chat_title = getattr(chat, 'title', '') or getattr(chat, 'username', '') or f"Chat#{chat_id}"
                    is_megagroup = getattr(chat, 'megagroup', False)
                    is_broadcast = getattr(chat, 'broadcast', False)
                    if is_megagroup or (not is_broadcast and chat_id < 0):
                        groups.append({
                            'id': chat_id,
                            'title': chat_title,
                            'entity': chat
                        })
            print(f"[启动] 账号 #{account_id} 加入 {len(groups)} 个群组")
            return groups
        except Exception as e:
            print(f"[启动] ❌ 账号 #{account_id} 获取群组列表失败: {str(e)}")
            return []
    
    async def _sync_all_groups(self, client: TelegramClient, account_id: int, group_list: list):
        """同步所有群组，确保能接收到消息更新"""
        if not group_list:
            return
        
        # 极致优化：增大批次，减少延迟
        batch_size = 50  # 从10增加到50，减少批次数量
        for i in range(0, len(group_list), batch_size):
            batch = group_list[i:i+batch_size]
            tasks = []
            
            for group_info in batch:
                async def sync_group(g):
                    try:
                        entity = g['entity']
                        await client.get_entity(entity)
                        try:
                            await client.get_messages(entity, limit=1)
                        except FloodWaitError as e:
                            await asyncio.sleep(e.seconds)
                            await client.get_messages(entity, limit=1)
                        except Exception:
                            pass
                        return True
                    except FloodWaitError as e:
                        await asyncio.sleep(e.seconds)
                        try:
                            await client.get_entity(g['entity'])
                            return True
                        except Exception:
                            return False
                    except Exception:
                        return False
                
                tasks.append(sync_group(group_info))
            
            await asyncio.gather(*tasks, return_exceptions=True)
            # 移除批次间延迟，全速运行
            # if i + batch_size < len(group_list):
            #     await asyncio.sleep(0.5)
    
    async def _notify_user_waiting(self, account_id: int, wait_seconds: int, reason: str = "加载中"):
        """通知用户需要等待"""
        try:
            target = settings_service.get_target_chat()
            if target and target.strip() and self.bot:
                account = dao_accounts.get(account_id)
                account_name = account.get('nickname') or account.get('username') or f"账号 #{account_id}"
                message = f"⏳ {account_name} {reason}，需要等待约 {wait_seconds} 秒，请稍候..."
                await self.bot.send_message(target, message)
        except Exception:
            pass
    
    async def _active_polling_task(self, client: TelegramClient, account_id: int, group_list: list):
        """主动轮询任务：定期检查新消息（防止漏消息）
        优化：将群组分成多个块，每个块在独立协程中并发检查
        每个账号的群组数/10=协程数，每个协程处理约10个群组
        """
        last_message_ids = {}
        # 极致优化：快速初始化，所有群组并发初始化，不等待完成
        # 使用信号量控制并发度，但最大化并发数
        init_semaphore = asyncio.Semaphore(200)  # 允许200个并发初始化
        
        async def init_group(g):
            async with init_semaphore:
                try:
                    entity = g['entity']
                    messages = await client.get_messages(entity, limit=1)
                    if messages:
                        last_message_ids[g['id']] = messages[0].id
                    else:
                        last_message_ids[g['id']] = 0
                except FloodWaitError as e:
                    await self._notify_user_waiting(account_id, e.seconds, f"初始化群组 '{g['title']}'")
                    await asyncio.sleep(e.seconds)
                    try:
                        messages = await client.get_messages(entity, limit=1)
                        if messages:
                            last_message_ids[g['id']] = messages[0].id
                        else:
                            last_message_ids[g['id']] = 0
                    except Exception:
                        last_message_ids[g['id']] = 0
                except Exception:
                    last_message_ids[g['id']] = 0
        
        # 所有群组并发初始化，不等待完成，立即开始轮询
        # 使用 create_task 让初始化在后台进行
        init_tasks = [asyncio.create_task(init_group(g)) for g in group_list]
        print(f"[轮询优化] 账号 #{account_id}: 启动 {len(init_tasks)} 个群组的后台初始化，立即开始轮询...")
        
        # 极致优化：每个群组独立协程，不分组，真正并发
        # 使用信号量控制并发度，但最大化并发数
        total_groups = len(group_list)
        poll_semaphore = asyncio.Semaphore(500)  # 允许500个并发轮询任务
        
        print(f"[轮询优化] 账号 #{account_id}: 共 {total_groups} 个群组，每个群组独立协程，极致并发（最大500并发）")
        
        # 全速运行：不考虑封号，极致性能，榨干CPU和内存
        poll_interval = 0.01  # 每个群组0.01秒轮询间隔，极致速度（10倍提升）
        floodwait_count = 0
        last_floodwait_time = 0
        
        # 定义检查单个群组的持续运行函数（每个群组独立协程）
        async def check_group_loop(group_info):
            """每个群组独立的持续运行协程"""
            chat_id = group_info['id']
            group_title = group_info.get('title', f'Group#{chat_id}')
            
            while True:
                try:
                    if not client.is_connected():
                        break
                    
                    entity = group_info['entity']
                    # 如果还没初始化完成，使用0作为last_id
                    last_id = last_message_ids.get(chat_id, 0)
                    
                    # 信号量只用于 API 调用，不阻塞消息处理
                    try:
                        async with poll_semaphore:  # 只控制 API 调用的并发度
                            # 极致优化：只获取最新3条消息，减少数据传输，提升速度
                            messages = await client.get_messages(entity, min_id=last_id, limit=3)
                    except FloodWaitError as e:
                        wait_seconds = e.seconds
                        await self._notify_user_waiting(account_id, wait_seconds, f"检查群组 '{group_title}'")
                        await asyncio.sleep(wait_seconds)
                        if not client.is_connected():
                            break
                        async with poll_semaphore:
                            messages = await client.get_messages(entity, min_id=last_id, limit=3)
                    except (ConnectionError, RuntimeError) as e:
                        if 'disconnected' in str(e).lower() or 'Cannot send requests' in str(e):
                            break
                        await asyncio.sleep(1)  # 出错后等待1秒再继续
                        continue
                    
                    # 消息处理在信号量外进行，确保真正并发
                    if messages:
                        # 极致优化：立即处理每条消息，不等待，不阻塞，真正并发
                        for msg in reversed(messages):
                            if msg.id > last_id and not msg.out:
                                try:
                                    class MockEvent:
                                            def __init__(self, msg_obj, chat_entity, chat_id_val, client_obj):
                                                self.message = msg_obj
                                                self.chat_id = chat_id_val
                                                self.client = client_obj
                                                self._chat_entity = chat_entity
                                                self._msg_obj = msg_obj
                                                self.is_private = False
                                                is_megagroup = getattr(chat_entity, 'megagroup', False)
                                                is_broadcast = getattr(chat_entity, 'broadcast', False)
                                                self.is_group = is_megagroup or (not is_broadcast and chat_id_val < 0)
                                                self.is_channel = is_broadcast
                                                self.out = getattr(msg_obj, 'out', False)
                                            
                                            async def get_chat(self):
                                                return self._chat_entity
                                            
                                            async def get_sender(self):
                                                if hasattr(self._msg_obj, 'from_id') and self._msg_obj.from_id:
                                                    try:
                                                        return await self.client.get_entity(self._msg_obj.from_id)
                                                    except:
                                                        return None
                                                return None
                                            
                                            async def click(self, row_idx, col_idx):
                                                """点击按钮（MockEvent 版本）"""
                                                try:
                                                    buttons = getattr(self.message, 'buttons', None)
                                                    if not buttons:
                                                        raise ValueError("消息没有按钮")
                                                    if row_idx >= len(buttons):
                                                        raise IndexError(f"行索引 {row_idx} 超出范围")
                                                    row = buttons[row_idx]
                                                    if col_idx >= len(row):
                                                        raise IndexError(f"列索引 {col_idx} 超出范围")
                                                    button = row[col_idx]
                                                    from telethon.tl.types import KeyboardButtonCallback, KeyboardButtonUrl, KeyboardButton
                                                    from telethon.tl.custom import MessageButton
                                                    if isinstance(button, (KeyboardButtonCallback, MessageButton)):
                                                        if hasattr(button, 'data'):
                                                            from telethon.tl.functions.messages import GetBotCallbackAnswerRequest
                                                            result = await self.client(GetBotCallbackAnswerRequest(
                                                                peer=self._chat_entity,
                                                                msg_id=self.message.id,
                                                                data=button.data
                                                            ))
                                                            return result
                                                        else:
                                                            raise ValueError("按钮没有回调数据")
                                                    elif isinstance(button, KeyboardButtonUrl):
                                                        raise ValueError("URL 按钮无法通过 API 点击")
                                                    else:
                                                        raise ValueError(f"不支持的按钮类型: {type(button)}")
                                                except Exception as e:
                                                    raise Exception(f"点击按钮失败: {str(e)}")
                                        
                                    mock_event = MockEvent(msg, entity, chat_id, client)
                                    
                                    if mock_event.is_group:
                                        # 极致优化：立即处理消息，不等待，不阻塞，真正并发
                                        # 使用 create_task 异步执行，立即调度，不阻塞其他群组
                                        # 检测到关键词后立即推送，不等待整个轮询周期
                                        # 不在信号量内，确保消息处理真正并发
                                        asyncio.create_task(self._process_message(mock_event, account_id, "ActivePolling"))
                                    
                                    # 立即更新 last_id，不等待消息处理完成
                                    last_message_ids[chat_id] = msg.id
                                except Exception as e:
                                    last_message_ids[chat_id] = msg.id
                        
                        # 更新最后的消息ID
                        if messages:
                            last_message_ids[chat_id] = max(msg.id for msg in messages)
                    
                    # 每个群组独立轮询间隔（在信号量外，不阻塞其他群组）
                    await asyncio.sleep(poll_interval)
                    
                except (ConnectionError, RuntimeError) as e:
                    if 'disconnected' in str(e).lower() or 'Cannot send requests' in str(e):
                        break
                    await asyncio.sleep(1)
                except (GeneratorExit, asyncio.CancelledError):
                    break
                except Exception as e:
                    await asyncio.sleep(1)
        
        # 启动所有群组的独立协程（每个群组一个持续运行的协程）
        print(f"[轮询优化] 账号 #{account_id}: 启动 {total_groups} 个群组的独立持续运行协程...")
        group_tasks = [asyncio.create_task(check_group_loop(g)) for g in group_list]
        
        # 等待所有任务完成（实际上它们会持续运行直到客户端断开）
        try:
            await asyncio.gather(*group_tasks, return_exceptions=True)
        except (GeneratorExit, asyncio.CancelledError):
            # 取消所有任务
            for task in group_tasks:
                task.cancel()
            await asyncio.gather(*group_tasks, return_exceptions=True)
        except Exception as e:
            print(f"[轮询] 账号 #{account_id} 轮询任务出错: {e}")
            import traceback
            traceback.print_exc()
    
    async def load_active_accounts(self):
        """加载所有活跃账号（支持多账号并发启动）"""
        all_rows = dao_accounts.list_all()
        print(f"[启动] 数据库中共有 {len(all_rows)} 个账号")
        
        # 输出所有账号的状态信息（用于调试）
        if all_rows:
            print("[启动] 账号状态详情：")
            for r in all_rows:
                acc_id = r.get('id', '?')
                status = r.get('status', 'NULL')
                phone = r.get('phone', 'N/A')
                nickname = r.get('nickname', 'N/A')
                print(f"  - 账号 #{acc_id}: status={status}, phone={phone}, nickname={nickname}")
        
        # 筛选活跃账号（status='active' 或 status 为 None/空，默认视为 active）
        rows = []
        for r in all_rows:
            status = r.get('status')
            if status == 'active' or status is None or status == '':
                rows.append(r)
        
        if not rows:
            print("[启动] ⚠️ 没有找到活跃账号（status='active' 或为空）")
            if all_rows:
                print("[启动] 💡 提示：如果账号状态不是 'active'，可以使用机器人命令查看账号列表")
            return
        
        print(f"[启动] 发现 {len(rows)} 个活跃账号，开始并发加载...")
        tasks = [self.start_account_client(row) for row in rows]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        success_count = 0
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                print(f"[启动] ❌ 账号 #{rows[i]['id']} 加载失败: {str(result)}")
                import traceback
                traceback.print_exc()
            else:
                success_count += 1
        
        print(f"[启动] ✅ 成功加载 {success_count}/{len(rows)} 个账号")

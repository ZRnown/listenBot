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

    async def start_control_bot(self):
        # 如果 bot 已存在，先断开连接
        if self.bot:
            try:
                await self.bot.disconnect()
            except Exception:
                pass
            self.bot = None
        
        session_file = 'control_bot.session'
        if os.path.exists(session_file):
            os.remove(session_file)
        self.bot = TelegramClient('control_bot', self.api_id, self.api_hash)
        await self.bot.start(bot_token=self.bot_token)
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
        await client.start(phone=lambda: None, password=lambda: None, code_callback=lambda: None)
        if not await client.is_user_authorized():
            await client.disconnect()
            raise RuntimeError('Session not authorized or requires login')
        me = await client.get_me()
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
            return {
                'id': account_id,
                'phone': phone,
                'username': f"@{username}" if username else None,
                'nickname': nickname.strip(),
                'existing': True
            }
        else:
            account_id = dao_accounts.create(phone, nickname.strip(), username, file_path, status='active')
            self._register_handlers_for_account(client, account_id)
            self.account_clients[account_id] = client
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
        await client.start(phone=lambda: None, password=lambda: None, code_callback=lambda: None)
        try:
            if not await client.is_user_authorized():
                raise RuntimeError('Session 未授权或需要登录')
        except Exception:
            await client.disconnect()
            raise
        me = await client.get_me()
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
            return {
                'id': account_id,
                'phone': phone,
                'username': f"@{username}" if username else None,
                'nickname': nickname.strip(),
                'existing': True
            }
        else:
            account_id = dao_accounts.create(phone, nickname.strip(), username, session_str, status='active')
            self._register_handlers_for_account(client, account_id)
            self.account_clients[account_id] = client
            return {
                'id': account_id,
                'phone': phone,
                'username': f"@{username}" if username else None,
                'nickname': nickname.strip(),
                'existing': False
            }

    def _register_handlers_for_account(self, client: TelegramClient, account_id: int, group_list: list = None):
        """为账号注册事件处理器（支持多账号并发）"""
        if group_list:
            group_ids_set = {g['id'] for g in group_list}
            client._monitored_group_ids = group_ids_set
        else:
            client._monitored_group_ids = None
        
        @client.on(events.NewMessage(incoming=True))
        async def handle_new_message(event):
            if group_list and hasattr(client, '_monitored_group_ids'):
                if event.chat_id not in client._monitored_group_ids:
                    return
            await self._process_message(event, account_id, "NewMessage")
        
        @client.on(events.MessageEdited(incoming=True))
        async def handle_message_edited(event):
            if group_list and hasattr(client, '_monitored_group_ids'):
                if event.chat_id not in client._monitored_group_ids:
                    return
            await self._process_message(event, account_id, "MessageEdited")
    
    async def _process_message(self, event, account_id: int, handler_name: str):
        """处理收到的消息（异步并发处理，不阻塞）"""
        try:
            # 快速过滤：只处理群组消息
            if event.is_private or not event.is_group:
                return
            
            account = dao_accounts.get(account_id)
            if account:
                # 异步处理，不阻塞事件循环
                asyncio.create_task(on_new_message(event, account, self.bot))
        except Exception as e:
            print(f"[处理消息] ❌ 账号 #{account_id} 错误: {str(e)}")

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
        asyncio.create_task(self._active_polling_task(client, account_id, group_list))
    
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
        
        batch_size = 10
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
            if i + batch_size < len(group_list):
                await asyncio.sleep(0.5)
    
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
        """主动轮询任务：定期检查新消息（防止漏消息）"""
        last_message_ids = {}
        for group_info in group_list:
            try:
                entity = group_info['entity']
                messages = await client.get_messages(entity, limit=1)
                if messages:
                    last_message_ids[group_info['id']] = messages[0].id
            except FloodWaitError as e:
                await self._notify_user_waiting(account_id, e.seconds, f"初始化群组 '{group_info['title']}'")
                await asyncio.sleep(e.seconds)
                try:
                    messages = await client.get_messages(entity, limit=1)
                    if messages:
                        last_message_ids[group_info['id']] = messages[0].id
                    else:
                        last_message_ids[group_info['id']] = 0
                except Exception:
                    last_message_ids[group_info['id']] = 0
            except Exception:
                last_message_ids[group_info['id']] = 0
        
        poll_interval = 3
        concurrent_limit = 35
        min_concurrent_limit = 15
        max_concurrent_limit = 40
        batch_delay = 0.03
        floodwait_count = 0
        last_floodwait_time = 0
        
        while True:
            try:
                await asyncio.sleep(poll_interval)
                
                if not client.is_connected():
                    break
                
                new_messages_count = 0
                total_groups = len(group_list)
                
                current_concurrent_limit = concurrent_limit
                if floodwait_count > 0:
                    time_since_floodwait = time.time() - last_floodwait_time if last_floodwait_time > 0 else 999
                    if time_since_floodwait < 300:
                        current_concurrent_limit = max(min_concurrent_limit, int(concurrent_limit * 0.7))
                    else:
                        floodwait_count = 0
                        current_concurrent_limit = concurrent_limit
                
                for batch_start in range(0, total_groups, current_concurrent_limit):
                    batch = group_list[batch_start:batch_start + current_concurrent_limit]
                    
                    async def check_group(group_info):
                        nonlocal floodwait_count, last_floodwait_time
                        try:
                            entity = group_info['entity']
                            chat_id = group_info['id']
                            last_id = last_message_ids.get(chat_id, 0)
                            
                            try:
                                messages = await client.get_messages(entity, min_id=last_id, limit=50)
                            except FloodWaitError as e:
                                wait_seconds = e.seconds
                                floodwait_count += 1
                                last_floodwait_time = time.time()
                                await self._notify_user_waiting(account_id, wait_seconds, f"检查群组 '{group_info['title']}'")
                                await asyncio.sleep(wait_seconds)
                                messages = await client.get_messages(entity, min_id=last_id, limit=50)
                            
                            group_new_count = 0
                            if messages:
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
                                                
                                                async def get_chat(self):
                                                    return self._chat_entity
                                                
                                                async def get_sender(self):
                                                    if hasattr(self._msg_obj, 'from_id') and self._msg_obj.from_id:
                                                        try:
                                                            return await self.client.get_entity(self._msg_obj.from_id)
                                                        except:
                                                            return None
                                                    return None
                                            
                                            mock_event = MockEvent(msg, entity, chat_id, client)
                                            
                                            if mock_event.is_group:
                                                await self._process_message(mock_event, account_id, "ActivePolling")
                                                group_new_count += 1
                                            
                                            last_message_ids[chat_id] = msg.id
                                        except Exception:
                                            last_message_ids[chat_id] = msg.id
                                            pass
                            
                            if messages:
                                last_message_ids[chat_id] = max(msg.id for msg in messages)
                            
                            return group_new_count
                        except Exception:
                            return 0
                    
                    tasks = [check_group(g) for g in batch]
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    for result in results:
                        if isinstance(result, int):
                            new_messages_count += result
                    
                    if batch_start + current_concurrent_limit < total_groups:
                        await asyncio.sleep(batch_delay)
                
                if new_messages_count > 0:
                    print(f"[轮询] 账号 #{account_id}: 发现 {new_messages_count} 条新消息")
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[轮询] 账号 #{account_id} 错误: {str(e)}")
    
    async def load_active_accounts(self):
        """加载所有活跃账号（支持多账号并发启动）"""
        all_rows = dao_accounts.list_all()
        rows = [r for r in all_rows if r.get('status') == 'active']
        if not rows:
            print("[启动] 没有活跃账号")
            return
        
        print(f"[启动] 发现 {len(rows)} 个活跃账号，开始并发加载...")
        tasks = [self.start_account_client(row) for row in rows]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        success_count = 0
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                print(f"[启动] ❌ 账号 #{rows[i]['id']} 加载失败: {str(result)}")
            else:
                success_count += 1
        
        print(f"[启动] ✅ 成功加载 {success_count}/{len(rows)} 个账号")

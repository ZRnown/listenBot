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

    async def start_control_bot(self):
        # 删除旧的会话文件，强制使用 bot_token 重新登录
        session_file = 'control_bot.session'
        if os.path.exists(session_file):
            os.remove(session_file)
        self.bot = TelegramClient('control_bot', self.api_id, self.api_hash)
        await self.bot.start(bot_token=self.bot_token)
        return self.bot

    async def stop(self):
        for c in list(self.account_clients.values()):
            await c.disconnect()
        self.account_clients.clear()
        if self.bot:
            await self.bot.disconnect()

    async def add_account_from_session_file(self, file_path: str):
        # create client from .session file path
        # Session path is used directly by Telethon if passed as string name without extension
        # use full absolute path without extension to ensure Telethon reads the correct file
        session_name = os.path.splitext(file_path)[0]
        client = TelegramClient(session_name, self.api_id, self.api_hash)
        # Use start() with no input to avoid interactive prompts
        await client.start(phone=lambda: None, password=lambda: None, code_callback=lambda: None)
        if not await client.is_user_authorized():
            await client.disconnect()
            raise RuntimeError('Session not authorized or requires login')
        me = await client.get_me()
        phone = getattr(me, 'phone', None)
        username = getattr(me, 'username', None)
        nickname = (getattr(me, 'first_name', '') or '') + ' ' + (getattr(me, 'last_name', '') or '')
        
        # 检查账号是否已存在
        existing = dao_accounts.find_by_phone_or_username(phone, username)
        if existing:
            # 账号已存在，更新 session_path 并返回现有账号ID
            account_id = existing['id']
            # 更新 session_path
            conn = cfg.pool.connection()
            cur = conn.cursor()
            try:
                cur.execute("UPDATE accounts SET session_path=%s, status='active' WHERE id=%s", (file_path, account_id))
                conn.commit()
            finally:
                cur.close()
                conn.close()
            # 如果客户端已存在，先断开
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
            # 新账号，创建记录
            account_id = dao_accounts.create(phone, nickname.strip(), username, file_path, status='active')
            # register handler
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
        # create client from StringSession (for listen/click accounts)
        try:
            sess = StringSession(session_str)
        except Exception:
            raise RuntimeError('无效的 StringSession 文本，请检查后重新发送')
        client = TelegramClient(sess, self.api_id, self.api_hash)
        # Use start() with no input to avoid interactive prompts
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
        
        # 检查账号是否已存在
        existing = dao_accounts.find_by_phone_or_username(phone, username)
        if existing:
            # 账号已存在，更新 session_path 并返回现有账号ID
            account_id = existing['id']
            # 更新 session_path（可能 StringSession 有更新）
            conn = cfg.pool.connection()
            cur = conn.cursor()
            try:
                cur.execute("UPDATE accounts SET session_path=%s, status='active' WHERE id=%s", (session_str, account_id))
                conn.commit()
            finally:
                cur.close()
                conn.close()
            # 如果客户端已存在，先断开
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
            # 新账号，创建记录
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
        from datetime import datetime
        print(f"[启动日志] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 为账号 #{account_id} 注册事件处理器（NewMessage + MessageEdited）")
        
        # 保存群组列表到客户端，用于后续过滤
        if group_list:
            # 创建一个集合，用于快速查找
            group_ids_set = {g['id'] for g in group_list}
            client._monitored_group_ids = group_ids_set
            print(f"[启动日志] 账号 #{account_id}: 已记录 {len(group_ids_set)} 个群组/频道的 ID 用于过滤")
        else:
            client._monitored_group_ids = None
        
        # 方案一：优化监听器 - 去掉 incoming=True，监听所有消息（包括自己发的）
        # 这样可以确保不会漏掉任何消息，即使消息是自己发的
        @client.on(events.NewMessage)  # 去掉 incoming=True，监听所有消息
        async def handle_new_message(event):
            # 如果提供了群组列表，检查是否在列表中
            if group_list and hasattr(client, '_monitored_group_ids'):
                chat_id = event.chat_id
                if chat_id not in client._monitored_group_ids:
                    # 不在监控列表中，跳过
                    return
            await self._process_message(event, account_id, "NewMessage")
        
        # 方案一：监听消息编辑事件 - 处理消息被修改的情况
        @client.on(events.MessageEdited)
        async def handle_message_edited(event):
            # 如果提供了群组列表，检查是否在列表中
            if group_list and hasattr(client, '_monitored_group_ids'):
                chat_id = event.chat_id
                if chat_id not in client._monitored_group_ids:
                    # 不在监控列表中，跳过
                    return
            await self._process_message(event, account_id, "MessageEdited")
        
        print(f"[启动日志] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 账号 #{account_id} 事件处理器注册完成（监听所有消息+消息编辑，自动过滤群组）")
    
    async def _process_message(self, event, account_id: int, handler_name: str):
        """处理收到的消息"""
        from datetime import datetime
        try:
            # 记录所有收到的消息（用于调试）
            chat_id = event.chat_id
            is_private = event.is_private
            is_group = event.is_group  # 使用 Telethon 的内置属性
            message_id = event.message.id if hasattr(event.message, 'id') else None
            
            # 立即打印收到消息的日志（用于诊断）
            print(f"\n{'='*60}")
            print(f"[消息接收] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 账号 #{account_id} 收到消息！")
            print(f"[消息接收]   消息ID: {message_id}, Chat ID: {chat_id}")
            print(f"[消息接收]   私聊: {is_private}, 群组: {is_group}, 频道: {event.is_channel}")
            print(f"{'='*60}\n")
            
            # 获取群组信息
            try:
                chat = await event.get_chat()
                chat_type = type(chat).__name__
                chat_title = getattr(chat, 'title', '') or getattr(chat, 'username', '') or f"Chat#{chat_id}"
                chat_username = getattr(chat, 'username', None)
                is_megagroup = getattr(chat, 'megagroup', False)
                is_broadcast = getattr(chat, 'broadcast', False)
                
                # 详细日志
                print(f"[事件接收] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [{handler_name}] 账号 #{account_id} 收到消息")
                print(f"[事件接收]   群组: '{chat_title}' (ID: {chat_id})")
                print(f"[事件接收]   类型: {chat_type}, 私聊: {is_private}, 群组: {is_group}, 超级群: {is_megagroup}, 频道: {is_broadcast}")
                print(f"[事件接收]   用户名: @{chat_username if chat_username else 'N/A'}")
            except Exception as e:
                print(f"[事件接收] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [{handler_name}] 账号 #{account_id} 收到消息 (Chat ID: {chat_id})")
                print(f"[事件接收]   无法获取群组信息: {str(e)}")
                chat_title = f"Chat#{chat_id}"
            
            # 只处理群组消息，忽略私聊和频道（如果需要监听频道，可以修改这里）
            if event.is_private:
                print(f"[事件接收]   跳过私聊消息")
                return
            
            # 使用 event.is_group 来判断是否是群组（包括普通群和超级群）
            if not event.is_group:
                print(f"[事件接收]   跳过非群组消息（可能是频道）")
                return
            
            # 方案三：处理话题群组 (Forum/Topics)
            topic_id = None
            if hasattr(event.message, 'reply_to') and event.message.reply_to:
                if hasattr(event.message.reply_to, 'forum_topic') and event.message.reply_to.forum_topic:
                    # 这是一个话题消息
                    topic_id = event.message.reply_to.reply_to_msg_id
                    print(f"[事件接收]   话题群组消息 - 话题ID: {topic_id}")
            
            # 处理群组消息
            try:
                account = dao_accounts.get(account_id)
                if account:
                    print(f"[事件接收]   开始处理群组消息...")
                    await on_new_message(event, account, self.bot)
                else:
                    print(f"[监听日志] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⚠️ 账号 #{account_id} 不存在于数据库中，无法处理消息")
            except Exception as e:
                print(f"[事件接收] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ❌ 账号 #{account_id} 处理消息时发生错误: {str(e)}")
                import traceback
                traceback.print_exc()
        except Exception as e:
            print(f"[事件接收] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ❌ 账号 #{account_id} 处理消息时发生未预期错误: {str(e)}")
            import traceback
            traceback.print_exc()

    async def start_account_client(self, account_row):
        from datetime import datetime
        account_id = account_row['id']
        session_path = account_row['session_path']
        print(f"[启动日志] 账号 #{account_id}: 开始初始化客户端...")
        # decide whether it's a file path or a StringSession string
        if session_path and os.path.exists(session_path):
            # use full path without extension to match stored session file
            session_name = os.path.splitext(session_path)[0]
            client = TelegramClient(session_name, self.api_id, self.api_hash)
            print(f"[启动日志] 账号 #{account_id}: 使用会话文件: {session_path}")
        else:
            # treat stored value as StringSession
            try:
                sess = StringSession(session_path)
                print(f"[启动日志] 账号 #{account_id}: 使用 StringSession")
            except Exception as e:
                raise RuntimeError(f'存储的会话字符串无效，无法恢复该账号: {str(e)}')
            client = TelegramClient(sess, self.api_id, self.api_hash)
        # Use start() with no input to avoid interactive prompts
        print(f"[启动日志] 账号 #{account_id}: 正在连接...")
        await client.start(phone=lambda: None, password=lambda: None, code_callback=lambda: None)
        print(f"[启动日志] 账号 #{account_id}: 连接成功")
        
        # 先列出账号加入的所有群组（在注册事件处理器之前）
        group_list = await self._list_account_groups(client, account_id)
        
        # 不再进行主动回溯，只监听新消息
        # 同步所有群组，确保能接收到消息更新
        await self._sync_all_groups(client, account_id, group_list)
        
        # 尝试获取最新更新（catch up）
        try:
            print(f"[启动日志] 账号 #{account_id}: 正在获取最新更新...")
            await client.catch_up()
            print(f"[启动日志] 账号 #{account_id}: 已获取最新更新")
        except Exception as e:
            print(f"[启动日志] 账号 #{account_id}: 获取最新更新时出错（可忽略）: {str(e)}")
        
        # 注册事件处理器（监听所有消息，自动过滤群组）
        print(f"[启动日志] 账号 #{account_id}: 注册事件处理器...")
        self._register_handlers_for_account(client, account_id, group_list)
        self.account_clients[account_id] = client
        print(f"[启动日志] 账号 #{account_id}: 事件处理器注册完成，监听已启动")
        
        # 启动主动轮询任务（定期检查新消息）
        if group_list and len(group_list) > 0:
            print(f"[启动日志] 账号 #{account_id}: 启动主动轮询任务（每10秒检查一次，并发20个群组，确保不遗漏任何消息）...")
            asyncio.create_task(self._active_polling_task(client, account_id, group_list))
        
        # 添加一个测试：检查是否能接收到消息
        print(f"[启动日志] 账号 #{account_id}: ⚠️ 重要提示 - 如果账号在其他设备（如手机）上登录，Telegram 可能不会推送更新到此客户端")
        print(f"[启动日志] 账号 #{account_id}: 建议：在其他设备上退出登录，或使用 Telegram 的'登出其他设备'功能")

    async def _list_account_groups(self, client: TelegramClient, account_id: int):
        """列出账号加入的所有群组和频道，返回群组列表"""
        from datetime import datetime
        try:
            print(f"\n{'='*80}")
            print(f"[启动日志] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 账号 #{account_id}: 正在获取群组列表...")
            groups = []
            channels = []
            megagroups = []
            all_groups = []  # 保存所有群组信息用于同步
            total_count = 0
            
            async for dialog in client.iter_dialogs():
                if not dialog.is_user:  # 只获取群组和频道
                    total_count += 1
                    chat = dialog.entity
                    chat_id = chat.id
                    chat_title = getattr(chat, 'title', '') or getattr(chat, 'username', '') or f"Chat#{chat_id}"
                    chat_username = getattr(chat, 'username', None)
                    is_megagroup = getattr(chat, 'megagroup', False)
                    is_broadcast = getattr(chat, 'broadcast', False)
                    
                    group_info = {
                        'title': chat_title,
                        'id': chat_id,
                        'username': chat_username,
                        'is_megagroup': is_megagroup,
                        'is_broadcast': is_broadcast,
                        'entity': chat  # 保存实体用于同步
                    }
                    
                    all_groups.append(group_info)
                    
                    if is_broadcast:
                        channels.append(group_info)
                    elif is_megagroup:
                        megagroups.append(group_info)
                    else:
                        groups.append(group_info)
            
            print(f"[启动日志] 账号 #{account_id}: 群组统计")
            print(f"  总对话数（非私聊）: {total_count}")
            print(f"  超级群组: {len(megagroups)} 个")
            print(f"  频道: {len(channels)} 个")
            print(f"  普通群组: {len(groups)} 个")
            print(f"\n[启动日志] 账号 #{account_id}: 详细群组列表")
            
            # 显示超级群组
            if megagroups:
                print(f"\n  超级群组 ({len(megagroups)} 个):")
                for i, g in enumerate(megagroups[:50], 1):  # 最多显示50个
                    username_str = f" @{g['username']}" if g['username'] else ""
                    print(f"    {i}. {g['title']}{username_str} (ID: {g['id']})")
                if len(megagroups) > 50:
                    print(f"    ... 还有 {len(megagroups) - 50} 个超级群组未显示")
            
            # 显示频道
            if channels:
                print(f"\n  频道 ({len(channels)} 个):")
                for i, g in enumerate(channels[:50], 1):  # 最多显示50个
                    username_str = f" @{g['username']}" if g['username'] else ""
                    print(f"    {i}. {g['title']}{username_str} (ID: {g['id']})")
                if len(channels) > 50:
                    print(f"    ... 还有 {len(channels) - 50} 个频道未显示")
            
            # 显示普通群组
            if groups:
                print(f"\n  普通群组 ({len(groups)} 个):")
                for i, g in enumerate(groups[:50], 1):  # 最多显示50个
                    username_str = f" @{g['username']}" if g['username'] else ""
                    print(f"    {i}. {g['title']}{username_str} (ID: {g['id']})")
                if len(groups) > 50:
                    print(f"    ... 还有 {len(groups) - 50} 个普通群组未显示")
            
            if total_count == 0:
                print(f"  ⚠️ 账号 #{account_id} 未加入任何群组或频道")
            
            print(f"{'='*80}\n")
            
            return all_groups  # 返回所有群组列表用于同步
            
        except Exception as e:
            print(f"[启动日志] ❌ 账号 #{account_id} 获取群组列表失败: {str(e)}")
            import traceback
            traceback.print_exc()
            return []
    
    async def _fetch_recent_history(self, client: TelegramClient, account_id: int, group_list: list):
        """方案二：启动时主动回溯 - 主动拉取每个群最近的 N 条消息，填补启动前的空白"""
        from datetime import datetime
        if not group_list:
            return
        
        print(f"[主动回溯] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 账号 #{account_id}: 开始主动回溯（补漏模式）...")
        print(f"[主动回溯] 账号 #{account_id}: 将检查 {len(group_list)} 个群组，每个群组回溯最近 20 条消息")
        
        # 存储已处理的消息ID，避免重复处理
        processed_messages = set()  # (chat_id, message_id)
        
        total_fetched = 0
        total_processed = 0
        
        # 限制检查的群组数量，避免启动时间过长
        # 如果群组太多，可以只检查最近活跃的群组
        max_groups_to_check = min(100, len(group_list))  # 最多检查100个群组
        groups_to_check = group_list[:max_groups_to_check]
        
        if len(group_list) > max_groups_to_check:
            print(f"[主动回溯] 账号 #{account_id}: 群组数量较多（{len(group_list)}），将只检查最近活跃的 {max_groups_to_check} 个群组")
        
        for idx, group_info in enumerate(groups_to_check, 1):
            try:
                entity = group_info['entity']
                chat_id = group_info['id']
                chat_title = group_info['title']
                
                # 主动拉取该群最近的 20 条消息（可根据需求调整）
                # limit=20: 如果断线很久，可以设为 50 或 100
                try:
                    messages = await client.get_messages(entity, limit=20)
                    total_fetched += len(messages) if messages else 0
                    
                    if messages:
                        # 处理这些消息（从旧到新）
                        for msg in reversed(messages):
                            # 检查是否已处理过
                            msg_key = (chat_id, msg.id)
                            if msg_key in processed_messages:
                                continue
                            
                            # 只处理收到的消息（不是自己发的）
                            if not msg.out and not msg.is_private:
                                try:
                                    # 创建模拟事件对象
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
                                    
                                    # 只处理群组消息
                                    if mock_event.is_group:
                                        await self._process_message(mock_event, account_id, "ActiveBackfill")
                                        processed_messages.add(msg_key)
                                        total_processed += 1
                                        
                                except Exception as e:
                                    # 静默处理错误，避免日志过多
                                    pass
                    
                except Exception as e:
                    # 忽略单个群组的错误，继续检查其他群组
                    pass
                
                # 关键：每个群检查完，暂停 1-2 秒，防止触发 FloodWait
                if idx < len(groups_to_check):
                    await asyncio.sleep(1.5)  # 1.5秒延迟，安全且不会太慢
                
            except Exception as e:
                # 忽略错误，继续处理下一个群组
                pass
        
        print(f"[主动回溯] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 账号 #{account_id}: 主动回溯完成")
        print(f"[主动回溯] 账号 #{account_id}: 共获取 {total_fetched} 条消息，处理了 {total_processed} 条群组消息")
        print(f"[主动回溯] 账号 #{account_id}: 转入实时监听模式\n")
    
    async def _notify_user_waiting(self, account_id: int, wait_seconds: int, reason: str = "加载中"):
        """通知用户需要等待"""
        try:
            target = settings_service.get_target_chat()
            if target and target.strip() and self.bot:
                account = dao_accounts.get(account_id)
                account_name = account.get('nickname') or account.get('username') or f"账号 #{account_id}"
                message = f"⏳ {account_name} {reason}，需要等待约 {wait_seconds} 秒，请稍候..."
                await self.bot.send_message(target, message)
                print(f"[通知] 已通知用户：{account_name} 需要等待 {wait_seconds} 秒")
        except Exception as e:
            print(f"[通知] 发送等待通知失败: {str(e)}")
    
    async def _sync_all_groups(self, client: TelegramClient, account_id: int, group_list: list):
        """同步所有群组，确保能接收到消息更新"""
        from datetime import datetime
        if not group_list:
            print(f"[启动日志] 账号 #{account_id}: 无需同步（无群组）")
            return
        
        print(f"[启动日志] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 账号 #{account_id}: 开始同步 {len(group_list)} 个群组...")
        synced_count = 0
        failed_count = 0
        
        # 分批同步，避免一次性请求太多
        batch_size = 10
        for i in range(0, len(group_list), batch_size):
            batch = group_list[i:i+batch_size]
            tasks = []
            
            for group_info in batch:
                async def sync_group(g):
                    try:
                        # 方法1: 获取实体（触发同步）
                        entity = g['entity']
                        await client.get_entity(entity)
                        
                        # 方法2: 尝试获取最新的一条消息（触发消息同步）
                        try:
                            # 只获取最新的一条消息，不下载内容
                            messages = await client.get_messages(entity, limit=1)
                            if messages:
                                # 成功获取消息，说明群组已同步
                                return True
                        except FloodWaitError as e:
                            # 遇到 FloodWait，需要等待
                            wait_seconds = e.seconds
                            print(f"[启动日志]   同步群组 '{g['title']}' 时遇到 FloodWait，需要等待 {wait_seconds} 秒")
                            await self._notify_user_waiting(account_id, wait_seconds, f"同步群组 '{g['title']}'")
                            await asyncio.sleep(wait_seconds)
                            # 重试一次
                            try:
                                messages = await client.get_messages(entity, limit=1)
                                if messages:
                                    return True
                            except Exception:
                                pass
                        except Exception:
                            # 如果获取消息失败，至少实体已同步
                            pass
                        
                        return True
                    except FloodWaitError as e:
                        # 遇到 FloodWait，需要等待
                        wait_seconds = e.seconds
                        print(f"[启动日志]   同步群组 '{g['title']}' 时遇到 FloodWait，需要等待 {wait_seconds} 秒")
                        await self._notify_user_waiting(account_id, wait_seconds, f"同步群组 '{g['title']}'")
                        await asyncio.sleep(wait_seconds)
                        # 重试一次
                        try:
                            entity = g['entity']
                            await client.get_entity(entity)
                            return True
                        except Exception:
                            return False
                    except Exception as e:
                        print(f"[启动日志]   同步群组 '{g['title']}' (ID: {g['id']}) 失败: {str(e)}")
                        return False
                
                tasks.append(sync_group(group_info))
            
            # 并发执行一批同步任务
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for j, result in enumerate(results):
                if result is True:
                    synced_count += 1
                else:
                    failed_count += 1
                    group_info = batch[j]
                    print(f"[启动日志]   ⚠️ 群组 '{group_info['title']}' (ID: {group_info['id']}) 同步失败")
            
            # 每批之间稍作延迟，避免请求过快
            if i + batch_size < len(group_list):
                await asyncio.sleep(0.5)
        
        print(f"[启动日志] 账号 #{account_id}: 群组同步完成 - 成功: {synced_count}, 失败: {failed_count}, 总计: {len(group_list)}")
    
    async def _active_polling_task(self, client: TelegramClient, account_id: int, group_list: list):
        """主动轮询任务：定期检查每个群组的新消息"""
        from datetime import datetime
        # 存储每个群组最后检查的消息ID
        last_message_ids = {}  # chat_id -> message_id
        
        # 初始化：获取每个群组的最新消息ID
        print(f"[主动轮询] 账号 #{account_id}: 初始化轮询任务，记录 {len(group_list)} 个群组的最新消息ID...")
        for group_info in group_list:
            try:
                entity = group_info['entity']
                messages = await client.get_messages(entity, limit=1)
                if messages:
                    last_message_ids[group_info['id']] = messages[0].id
            except FloodWaitError as e:
                wait_seconds = e.seconds
                print(f"[主动轮询] 账号 #{account_id}: 初始化群组 '{group_info['title']}' 时遇到 FloodWait，需要等待 {wait_seconds} 秒")
                await self._notify_user_waiting(account_id, wait_seconds, f"初始化群组 '{group_info['title']}'")
                await asyncio.sleep(wait_seconds)
                # 重试一次
                try:
                    messages = await client.get_messages(entity, limit=1)
                    if messages:
                        last_message_ids[group_info['id']] = messages[0].id
                    else:
                        last_message_ids[group_info['id']] = 0
                except Exception:
                    last_message_ids[group_info['id']] = 0
            except Exception as e:
                print(f"[主动轮询] 账号 #{account_id}: 初始化群组 '{group_info['title']}' 失败: {str(e)}")
                last_message_ids[group_info['id']] = 0
        
        print(f"[主动轮询] 账号 #{account_id}: 初始化完成，已记录 {len(last_message_ids)} 个群组的最新消息ID")
        
        # 轮询间隔（秒）- 优化为3秒，更快检测新消息（严格控制避免封号）
        poll_interval = 3
        
        # 并发控制：同时检查的群组数量（动态调整，避免请求过快被封号）
        concurrent_limit = 35  # 每批最多35个群组并发检查（提高效率）
        min_concurrent_limit = 15  # 最小并发数（遇到限流时降低）
        max_concurrent_limit = 40  # 最大并发数（安全上限）
        
        # 批次间延迟（秒）- 优化为0.03秒，更快处理
        batch_delay = 0.03
        
        # FloodWait 计数器（用于动态调整）
        floodwait_count = 0
        last_floodwait_time = 0
        
        while True:
            try:
                await asyncio.sleep(poll_interval)
                
                # 检查客户端是否还在运行
                if not client.is_connected():
                    print(f"[主动轮询] 账号 #{account_id}: 客户端已断开，停止轮询")
                    break
                
                # 分批并发检查群组，提高效率
                new_messages_count = 0
                total_groups = len(group_list)
                
                # 动态调整并发数：如果最近遇到FloodWait，降低并发数
                current_concurrent_limit = concurrent_limit
                if floodwait_count > 0:
                    # 如果最近5分钟内遇到FloodWait，降低并发数
                    current_time = time.time()
                    time_since_floodwait = current_time - last_floodwait_time if last_floodwait_time > 0 else 999
                    if time_since_floodwait < 300:  # 5分钟内
                        current_concurrent_limit = max(min_concurrent_limit, int(concurrent_limit * 0.7))
                        print(f"[主动轮询] 账号 #{account_id}: 检测到最近有FloodWait，降低并发数至 {current_concurrent_limit}")
                    else:
                        # 5分钟后恢复正常
                        floodwait_count = 0
                        current_concurrent_limit = concurrent_limit
                
                # 分批处理群组
                for batch_start in range(0, total_groups, current_concurrent_limit):
                    batch = group_list[batch_start:batch_start + current_concurrent_limit]
                    
                    # 并发检查这一批群组
                    async def check_group(group_info):
                        nonlocal floodwait_count, last_floodwait_time
                        try:
                            entity = group_info['entity']
                            chat_id = group_info['id']
                            last_id = last_message_ids.get(chat_id, 0)
                            
                            # 获取比 last_id 更新的消息（增加limit确保不遗漏）
                            try:
                                messages = await client.get_messages(entity, min_id=last_id, limit=50)
                            except FloodWaitError as e:
                                wait_seconds = e.seconds
                                floodwait_count += 1
                                last_floodwait_time = time.time()
                                print(f"[主动轮询] 账号 #{account_id}: 检查群组 '{group_info['title']}' 时遇到 FloodWait，需要等待 {wait_seconds} 秒（累计 {floodwait_count} 次）")
                                await self._notify_user_waiting(account_id, wait_seconds, f"检查群组 '{group_info['title']}'")
                                await asyncio.sleep(wait_seconds)
                                # 重试一次
                                messages = await client.get_messages(entity, min_id=last_id, limit=50)
                            
                            group_new_count = 0
                            if messages:
                                # 处理新消息（从旧到新）
                                for msg in reversed(messages):
                                    if msg.id > last_id and not msg.out:  # 只处理收到的消息
                                        try:
                                            # 创建一个模拟的事件对象来触发处理
                                            class MockEvent:
                                                def __init__(self, msg_obj, chat_entity, chat_id_val, client_obj):
                                                    self.message = msg_obj
                                                    self.chat_id = chat_id_val
                                                    self.client = client_obj
                                                    self._chat_entity = chat_entity
                                                    self._msg_obj = msg_obj
                                                    self.is_private = False
                                                    # 判断是否是群组
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
                                            
                                            # 检查是否是群组消息
                                            if mock_event.is_group:
                                                await self._process_message(mock_event, account_id, "ActivePolling")
                                                group_new_count += 1
                                            
                                            # 更新最后的消息ID
                                            last_message_ids[chat_id] = msg.id
                                        except Exception as e:
                                            # 即使处理失败，也更新消息ID，避免重复处理
                                            last_message_ids[chat_id] = msg.id
                                            # 静默处理错误，避免日志过多
                                            pass
                            
                            # 更新最后的消息ID（即使没有新消息，也要更新，避免重复检查）
                            if messages:
                                last_message_ids[chat_id] = max(msg.id for msg in messages)
                            
                            return group_new_count
                        except Exception as e:
                            # 忽略单个群组的错误，继续检查其他群组
                            return 0
                    
                    # 并发执行这一批群组的检查
                    tasks = [check_group(g) for g in batch]
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    # 统计新消息数量
                    for result in results:
                        if isinstance(result, int):
                            new_messages_count += result
                    
                    # 批次之间稍作延迟，避免请求过快（优化为0.03秒，更快处理）
                    if batch_start + current_concurrent_limit < total_groups:
                        await asyncio.sleep(batch_delay)
                
                if new_messages_count > 0:
                    print(f"[主动轮询] 账号 #{account_id}: 本轮检查发现 {new_messages_count} 条新消息（检查了 {total_groups} 个群组）")
                    
            except asyncio.CancelledError:
                print(f"[主动轮询] 账号 #{account_id}: 轮询任务被取消")
                break
            except Exception as e:
                print(f"[主动轮询] 账号 #{account_id}: 轮询任务出错: {str(e)}")
                import traceback
                traceback.print_exc()
                # 继续运行，不要因为一次错误就停止

    async def load_active_accounts(self):
        from datetime import datetime
        rows = dao_accounts.list_all()
        print(f"[启动日志] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 开始加载活跃账号，共 {len(rows)} 个账号")
        loaded_count = 0
        for r in rows:
            if r['status'] == 'active':
                try:
                    print(f"[启动日志] 正在加载账号 #{r['id']} ({r.get('nickname', 'Unknown')})...")
                    await self.start_account_client(r)
                    loaded_count += 1
                    print(f"[启动日志] ✅ 账号 #{r['id']} 加载成功")
                except Exception as e:
                    print(f"[启动日志] ❌ 账号 #{r['id']} 加载失败: {str(e)}")
                    import traceback
                    traceback.print_exc()
                    continue
        print(f"[启动日志] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 账号加载完成，成功加载 {loaded_count}/{len([r for r in rows if r['status'] == 'active'])} 个账号")

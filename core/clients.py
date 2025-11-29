import asyncio
import os
import app.config as cfg
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from storage import dao_accounts
from services import sessions as sess_service
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

    def _register_handlers_for_account(self, client: TelegramClient, account_id: int):
        from datetime import datetime
        print(f"[启动日志] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 为账号 #{account_id} 注册 NewMessage 事件处理器")
        @client.on(events.NewMessage(incoming=True))
        async def handle_event(event):
            # 记录所有收到的消息（用于调试）
            try:
                chat = await event.get_chat()
                chat_id = event.chat_id
                chat_type = type(chat).__name__
                chat_title = getattr(chat, 'title', '') or getattr(chat, 'username', '') or f"Chat#{chat_id}"
                is_private = event.is_private
                is_group = getattr(chat, 'megagroup', False) or getattr(chat, 'broadcast', False) or (not is_private and chat_id < 0)
                print(f"[事件接收] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 账号 #{account_id} 收到消息 | 群组: {chat_title} (ID: {chat_id}, 类型: {chat_type}, 私聊: {is_private}, 群组: {is_group})")
            except Exception as e:
                print(f"[事件接收] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 账号 #{account_id} 收到消息 | 无法获取群组信息: {str(e)}")
            
            # 只处理群组消息，忽略私聊
            if event.is_private:
                return
            try:
                account = dao_accounts.get(account_id)
                if account:
                    await on_new_message(event, account, self.bot)
                else:
                    print(f"[监听日志] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⚠️ 账号 #{account_id} 不存在于数据库中，无法处理消息")
            except (GeneratorExit, RuntimeError) as e:
                # 忽略 Telethon 内部连接关闭时的错误
                if 'GeneratorExit' in str(type(e).__name__) or 'coroutine ignored' in str(e):
                    return
                # 其他错误打印日志
                print(f"[监听日志] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⚠️ 账号 #{account_id} 处理消息时发生 RuntimeError: {str(e)}")
            except Exception as e:
                print(f"[监听日志] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ❌ 账号 #{account_id} 处理消息时发生错误: {str(e)}")
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
        print(f"[启动日志] 账号 #{account_id}: 连接成功，注册事件处理器...")
        self._register_handlers_for_account(client, account_id)
        self.account_clients[account_id] = client
        print(f"[启动日志] 账号 #{account_id}: 事件处理器注册完成，监听已启动")

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

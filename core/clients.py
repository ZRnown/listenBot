import asyncio
import os
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
        account_id = dao_accounts.create(phone, nickname.strip(), username, file_path, status='active')
        # register handler
        self._register_handlers_for_account(client, account_id)
        self.account_clients[account_id] = client
        return {
            'id': account_id,
            'phone': phone,
            'username': f"@{username}" if username else None,
            'nickname': nickname.strip()
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
        # store the StringSession text in session_path field
        account_id = dao_accounts.create(phone, nickname.strip(), username, session_str, status='active')
        self._register_handlers_for_account(client, account_id)
        self.account_clients[account_id] = client
        return {
            'id': account_id,
            'phone': phone,
            'username': f"@{username}" if username else None,
            'nickname': nickname.strip()
        }

    def _register_handlers_for_account(self, client: TelegramClient, account_id: int):
        @client.on(events.NewMessage(incoming=True))
        async def handle_event(event):
            # 使用监听账号的客户端发送提醒（不再使用机器人客户端）
            account = dao_accounts.get(account_id)
            await on_new_message(event, account, self.bot)

    async def start_account_client(self, account_row):
        session_path = account_row['session_path']
        # decide whether it's a file path or a StringSession string
        if session_path and os.path.exists(session_path):
            # use full path without extension to match stored session file
            session_name = os.path.splitext(session_path)[0]
            client = TelegramClient(session_name, self.api_id, self.api_hash)
        else:
            # treat stored value as StringSession
            try:
                sess = StringSession(session_path)
            except Exception:
                raise RuntimeError('存储的会话字符串无效，无法恢复该账号')
            client = TelegramClient(sess, self.api_id, self.api_hash)
        # Use start() with no input to avoid interactive prompts
        await client.start(phone=lambda: None, password=lambda: None, code_callback=lambda: None)
        self._register_handlers_for_account(client, account_row['id'])
        self.account_clients[account_row['id']] = client

    async def load_active_accounts(self):
        rows = dao_accounts.list_all()
        for r in rows:
            if r['status'] == 'active':
                try:
                    await self.start_account_client(r)
                except Exception:
                    continue

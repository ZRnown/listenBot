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

    def _register_handlers_for_account(self, client: TelegramClient, account_id: int, group_list: list = None, register_listeners: bool = True):
        """
        为账号注册事件处理器。

        目前仅实现：
        - 全局监听所有群/频道的新消息
        - 如果消息包含按钮，则触发全体点击账号的自动点击任务

        不再实现任何“监听 + 转发”功能。
        """
        client._monitored_group_ids = None

        # 是否开启监听（目前只对指定账号开启）
        if not register_listeners:
            return

        # 初始化去重缓存：避免同一条消息被多个账号重复触发自动点击
        if not hasattr(self, "_auto_click_seen"):
            # (chat_id, msg_id) 组成的集合
            self._auto_click_seen = set()

        from bot.click_tasks import auto_click_on_message  # 延迟导入，避免循环依赖

        @client.on(events.NewMessage(incoming=True))
        async def _auto_click_handler(event):
            """
            监听所有群/频道消息，如果包含按钮，则触发一次全局自动点击任务。
            """
            try:
                # 只处理群/频道消息，忽略私聊
                if not (event.is_group or event.is_channel):
                    return

                msg = event.message
                buttons = getattr(msg, "buttons", None)
                if not buttons:
                    return

                chat_id = event.chat_id
                msg_id = event.id
                key = (chat_id, msg_id)

                # 去重：任意一个账号触发过这条消息，就不再重复触发
                if key in self._auto_click_seen:
                    return
                self._auto_click_seen.add(key)

                # 如果是专用监听账号（例如 #125），在群里输出一条监听日志
                if account_id == 125:
                    try:
                        # 收集按钮文本
                        btn_texts = []
                        for row in buttons:
                            for btn in row:
                                t = getattr(btn, "text", "") or ""
                                if t:
                                    btn_texts.append(t)
                        btn_preview = ", ".join(btn_texts[:5])
                        if len(btn_texts) > 5:
                            btn_preview += f" ... (共 {len(btn_texts)} 个按钮)"

                        msg_text = msg.message or ""
                        log_text = (
                            "📡 监听日志\n"
                            f"• Chat ID: {chat_id}\n"
                            f"• Message ID: {msg_id}\n"
                            f"• 文本：{msg_text[:500]}\n"
                            f"• 按钮：{btn_preview or '（无）'}"
                        )
                        await client.send_message(chat_id, log_text)
                    except Exception as e:
                        print(f"[自动点击监听] 账号 #{account_id} 输出监听日志失败: {e}")

                # 调用自动点击逻辑（不阻塞当前 handler）
                asyncio.create_task(auto_click_on_message(self, chat_id, msg_id))
            except Exception as e:
                # 避免异常中断 Telethon 的事件循环，只打印日志
                print(f"[自动点击监听] 账号 #{account_id} 处理消息时出错: {e}")

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
        print(f"[启动] 账号 #{account_id} 客户端已启动")

        # 只有指定账号（例如 #125）开启监听，其它账号只作为在线资源
        register_listeners = (account_id == 125)
        if register_listeners:
            print(f"[启动] 账号 #{account_id} 启用群消息监听（自动点击 + 日志）")
        self._register_handlers_for_account(client, account_id, None, register_listeners=register_listeners)
        self.account_clients[account_id] = client
        print(f"[启动] 账号 #{account_id} 客户端已就绪")

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
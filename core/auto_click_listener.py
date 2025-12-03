import asyncio
import random
import time
from typing import List, Optional, Tuple

from telethon import events
from telethon.errors import FloodWaitError

from core.filters import normalize_text_for_matching
from services import settings_service
from storage import dao_accounts


class AutoClickListener:
    """
    监听群消息中的按钮，根据账号的点击关键词自动触发点击。

    规则：
    - 仅处理群组/频道消息（忽略私聊）
    - 支持 listen_sources 白名单；为空时默认监听所有群
    - 单条消息仅点击一次，避免重复
    """

    KEYWORD_CACHE_TTL = 30
    SOURCE_CACHE_TTL = 60
    HISTORY_TTL = 3600

    def __init__(self, client, account_id: int):
        self.client = client
        self.account_id = account_id
        self._keywords_cache: Tuple[List[str], float] = ([], 0)
        self._sources_cache: Tuple[List[str], float] = ([], 0)
        self._click_history = {}
        self._account_label = self._resolve_account_label()

    def register(self):
        self.client.add_event_handler(self.on_new_message, events.NewMessage(incoming=True))
        print(f"[监听点击] 账号 {self._account_label} 已开启群内自动点击监听")

    def _resolve_account_label(self) -> str:
        try:
            row = dao_accounts.get(self.account_id)
            if row:
                return row.get('username') or row.get('phone') or f"#{self.account_id}"
        except Exception:
            pass
        return f"#{self.account_id}"

    async def on_new_message(self, event):
        try:
            if event.out or event.is_private:
                return

            message = event.message
            buttons = getattr(message, 'buttons', None) if message else None
            if not message or not buttons:
                return

            chat_id = event.chat_id
            if not chat_id:
                return

            chat = None
            chat_label = str(chat_id)
            try:
                chat = await event.get_chat()
                chat_label = getattr(chat, 'title', None) or getattr(chat, 'username', None) or str(chat_id)
            except Exception:
                pass

            # 检测到按钮，记录日志
            button_texts = []
            for row in buttons:
                for btn in row:
                    btn_text = getattr(btn, 'text', '') or ''
                    if btn_text:
                        button_texts.append(btn_text)
            print(
                f"[监听点击] 账号 {self._account_label} 检测到按钮 | "
                f"群组: {chat_label} | "
                f"消息ID: {event.message.id} | "
                f"按钮数量: {len(button_texts)} | "
                f"按钮文本: {', '.join(button_texts[:5])}{'...' if len(button_texts) > 5 else ''}"
            )

            if not self._is_source_allowed(chat, chat_id):
                print(f"[监听点击] 账号 {self._account_label} 群组 {chat_label} 不在监听白名单中，跳过")
                return

            keywords = self._get_click_keywords()
            if not keywords:
                print(f"[监听点击] 账号 {self._account_label} 未设置点击关键词，跳过")
                return

            coords = self._find_matching_button(message, keywords)
            if not coords:
                print(
                    f"[监听点击] 账号 {self._account_label} 未匹配到关键词 | "
                    f"群组: {chat_label} | "
                    f"当前关键词: {', '.join(keywords[:3])}{'...' if len(keywords) > 3 else ''}"
                )
                return

            row_idx, col_idx, btn_text, matched_keyword = coords
            if self._already_clicked(chat_id, message.id):
                print(
                    f"[监听点击] 账号 {self._account_label} 已点击过该消息，跳过 | "
                    f"群组: {chat_label} | "
                    f"消息ID: {event.message.id} | "
                    f"按钮: '{btn_text}' (关键词: {matched_keyword})"
                )
                return

            print(
                f"[监听点击] 账号 {self._account_label} 匹配到关键词，准备点击 | "
                f"群组: {chat_label} | "
                f"消息ID: {event.message.id} | "
                f"按钮: '{btn_text}' (位置: [{row_idx},{col_idx}]) | "
                f"关键词: {matched_keyword}"
            )

            success = await self._click_button(message, row_idx, col_idx, btn_text, matched_keyword, chat)
            if success:
                self._remember_click(chat_id, message.id)
        except Exception as e:
            print(f"[监听点击] 账号 {self._account_label} 处理消息失败: {e}")

    def _get_click_keywords(self) -> List[str]:
        now = time.time()
        keywords, ts = self._keywords_cache
        if now - ts > self.KEYWORD_CACHE_TTL:
            keywords = settings_service.get_account_keywords(self.account_id, kind='click') or []
            self._keywords_cache = (keywords, now)
        cleaned = []
        for k in keywords:
            t = (k or '').strip()
            if t:
                cleaned.append(t)
        return cleaned

    def _get_listen_sources(self) -> List[str]:
        now = time.time()
        sources, ts = self._sources_cache
        if now - ts > self.SOURCE_CACHE_TTL:
            sources = settings_service.get_listen_sources(self.account_id) or []
            self._sources_cache = (sources, now)
        return sources

    def _is_source_allowed(self, chat, chat_id: int) -> bool:
        sources = self._get_listen_sources()
        if not sources:
            return True

        normalized_targets = set()
        raw_targets = set()
        for src in sources:
            s = (src or '').strip()
            if not s:
                continue
            raw_targets.add(s)
            normalized = s.lower()
            normalized = normalized.replace('https://t.me/', '')
            normalized = normalized.replace('http://t.me/', '')
            normalized = normalized.replace('t.me/', '')
            normalized = normalized.lstrip('@')
            normalized_targets.add(normalized)

        chat_username = ''
        chat_title = ''
        if chat:
            chat_username = (getattr(chat, 'username', None) or '').lower()
            chat_title = (getattr(chat, 'title', None) or '').lower()
        chat_id_str = str(chat_id)
        chat_id_abs_str = str(abs(chat_id))

        candidates = set()
        if chat_username:
            candidates.update({
                chat_username,
                f"@{chat_username}",
                f"t.me/{chat_username}",
            })
        if chat_title:
            candidates.add(chat_title)
        candidates.update({chat_id_str, chat_id_abs_str})

        for c in list(candidates):
            if c.lower() in normalized_targets or c in raw_targets:
                return True

        return False

    def _find_matching_button(self, message, keywords: List[str]):
        buttons = getattr(message, 'buttons', None) or []
        for row_idx, row in enumerate(buttons):
            for col_idx, btn in enumerate(row):
                btn_text = getattr(btn, 'text', '') or ''
                normalized_btn = normalize_text_for_matching(btn_text)
                if not normalized_btn:
                    continue
                for kw in keywords:
                    if kw and kw in normalized_btn:
                        return row_idx, col_idx, btn_text, kw
        return None

    async def _click_button(self, message, row_idx: int, col_idx: int, btn_text: str, keyword: str, chat):
        delay = settings_service.get_click_delay(self.account_id)
        jitter = settings_service.get_click_jitter()
        actual_delay = delay + random.uniform(-jitter, jitter)
        if actual_delay > 0:
            await asyncio.sleep(actual_delay)

        try:
            await message.click(row=row_idx, column=col_idx)
            chat_title = getattr(chat, 'title', None) if chat else ''
            chat_label = chat_title or getattr(chat, 'username', None) or str(message.chat_id)
            print(
                f"[监听点击] 账号 {self._account_label} 在 {chat_label} 点击按钮 "
                f"'{btn_text}' (关键词: {keyword}, 延迟: {actual_delay:.2f}s)"
            )
            return True
        except FloodWaitError as e:
            print(f"[监听点击] 账号 {self._account_label} 触发 FloodWait: {e}")
        except Exception as e:
            print(f"[监听点击] 账号 {self._account_label} 点击按钮失败: {e}")
        return False

    def _already_clicked(self, chat_id: int, message_id: int) -> bool:
        now = time.time()
        self._cleanup_history(now)
        key = (chat_id, message_id)
        return key in self._click_history

    def _remember_click(self, chat_id: int, message_id: int):
        self._click_history[(chat_id, message_id)] = time.time()

    def _cleanup_history(self, now: Optional[float] = None):
        now = now or time.time()
        expired = [k for k, ts in self._click_history.items() if now - ts > self.HISTORY_TTL]
        for k in expired:
            self._click_history.pop(k, None)


def register_auto_click_listener(client, account_id: int):
    listener = AutoClickListener(client, account_id)
    listener.register()
    return listener


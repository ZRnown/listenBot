import asyncio
import random
from core.filters import match_keywords
from services.alerting import send_alert
from services import settings_service

# per-account concurrency control
_ACCOUNT_SEMAPHORES: dict[int, tuple[asyncio.Semaphore, int]] = {}


def _get_semaphore(account_id: int) -> asyncio.Semaphore:
    value = max(1, settings_service.get_concurrency(account_id))
    sem, current = _ACCOUNT_SEMAPHORES.get(account_id, (None, 0))
    if sem is None or current != value:
        sem = asyncio.Semaphore(value)
        _ACCOUNT_SEMAPHORES[account_id] = (sem, value)
    return sem


async def on_new_message(event, account: dict, bot_client):
    try:
        # bot_client 参数保留以保持兼容性，但不再使用（现在使用监听账号的客户端发送）
        
        text = event.message.message or ''
        role = settings_service.get_account_role(account['id']) or 'both'

        # 1) 关键词提醒（仅当角色包含 listen）
        if role in ('listen', 'both'):
            # listen whitelist filtering
            sources = settings_service.get_listen_sources(account['id']) or []
            if sources:
                # build possible identifiers for current chat
                candidates = set()
                try:
                    cid = str(event.chat_id)
                    candidates.add(cid)
                    # 对于超级群组/频道，chat_id 是负数，也尝试添加绝对值
                    try:
                        cid_int = int(cid)
                        if cid_int < 0:
                            candidates.add(str(abs(cid_int)))
                            # 对于 -100xxxxxxxxxx 格式，也添加 xxxxxxxx
                            if str(cid_int).startswith('-100'):
                                candidates.add(str(cid_int)[4:])
                    except Exception:
                        pass
                except Exception:
                    pass
                try:
                    ent = await event.get_chat()
                    uname = getattr(ent, 'username', None)
                    if uname:
                        candidates.add('@' + uname)
                        candidates.add(uname)  # 不带 @
                        candidates.add(f't.me/{uname}')
                        candidates.add(f'https://t.me/{uname}')
                except Exception:
                    pass
                # 检查是否匹配（支持多种格式）
                matched_source = False
                for src in sources:
                    if not src or not src.strip():
                        continue
                    src_clean = src.strip()
                    # 直接匹配
                    if src_clean in candidates:
                        matched_source = True
                        break
                    # 处理 @username 格式
                    if src_clean.startswith('@'):
                        if src_clean[1:] in candidates or src_clean in candidates:
                            matched_source = True
                            break
                    # 处理 t.me/username 格式
                    if 't.me/' in src_clean:
                        username_part = src_clean.split('t.me/')[-1].split('/')[0].split('?')[0]
                        if username_part in candidates or f'@{username_part}' in candidates:
                            matched_source = True
                            break
                    # 处理 chat_id 匹配
                    try:
                        src_id = int(src_clean)
                        if str(src_id) in candidates or str(-src_id) in candidates:
                            matched_source = True
                            break
                    except Exception:
                        pass
                
                # 如果设置了监听源但没有匹配，跳过处理
                if not matched_source:
                    return
            
            # 检查关键词匹配
            keywords = settings_service.get_account_keywords(account['id'], kind='listen') or []
            if not keywords:
                # 如果没有设置关键词，不处理
                return
            
            matched = match_keywords(account['id'], text, kind='listen')
            if matched:
                # 检查转发目标（只使用账号专属的，不使用全局的）
                target = settings_service.get_account_target_chat(account['id'])
                if not target or not target.strip():
                    print(f"[DEBUG] 账号 {account['id']} 匹配到关键词 '{matched}'，但未设置转发目标")
                    return
                
                # 使用监听账号的客户端发送（而不是机器人客户端）
                account_client = event.client
                print(f"[DEBUG] 账号 {account['id']} 匹配到关键词 '{matched}'，准备使用监听账号发送到 {target}")
                await send_alert(account_client, account, event, matched)
                # optional: start sending template message
                if settings_service.get_start_sending(account['id']):
                    tpl = settings_service.get_template_message(account['id'])
                    if tpl:
                        delay = settings_service.get_send_delay(account['id'])
                        jitter = settings_service.get_send_jitter()
                        async def _send():
                            try:
                                if delay and delay > 0:
                                    # add small random jitter to avoid patterns
                                    await asyncio.sleep(max(0.0, delay + random.uniform(-jitter, jitter)))
                                await event.client.send_message(event.chat_id, tpl)
                            except Exception:
                                pass
                        sem = _get_semaphore(account['id'])
                        async def _runner():
                            async with sem:
                                await _send()
                        # do not block handler
                        asyncio.create_task(_runner())

        # 2) 按钮点击（仅当角色包含 click）
        if role not in ('click', 'both'):
            return
        buttons = getattr(event.message, 'buttons', None)
        if not buttons:
            return
        # buttons is List[List[Button]]
        keywords = settings_service.get_account_keywords(account['id'], kind='click') or []
        if not keywords:
            return
        # 遍历按钮，查找命中
        for i, row in enumerate(buttons):
            for j, btn in enumerate(row):
                btn_text = getattr(btn, 'text', None) or ''
                if any(k for k in keywords if k and k in btn_text):
                    # 点击延迟
                    delay = settings_service.get_click_delay(account['id'])
                    jitter = settings_service.get_click_jitter()
                    if delay and delay > 0:
                        await asyncio.sleep(max(0.0, delay + random.uniform(-jitter, jitter)))
                    # 判定 Inline vs Reply 按钮
                    try:
                        # 优先尝试 inline 点击（有 callback 的）
                        await event.click(i, j)
                    except Exception:
                        # 退化为发送按钮文本（reply keyboard）
                        try:
                            await event.client.send_message(event.chat_id, btn_text)
                        except Exception:
                            pass
                    return
    except Exception:
        # best-effort; do not raise
        pass

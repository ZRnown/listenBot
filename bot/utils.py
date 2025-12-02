"""
工具函数模块 - 状态管理、命令匹配等通用功能
"""
import re
import unicodedata
from typing import Optional

# 简单会话状态管理
STATE = {}
# chat_id -> { 'mode': str, 'pending': {...} }


def set_state(chat_id, mode=None, **pending):
    if mode is None:
        STATE.pop(chat_id, None)
        return
    STATE[chat_id] = {'mode': mode, 'pending': pending}


def get_state(chat_id):
    return STATE.get(chat_id)


def _strip_emoji_prefix(value: str) -> str:
    s = (value or '').strip()
    while s:
        cat = unicodedata.category(s[0])
        if cat not in ('So', 'Sk', 'Cn'):
            break
        s = s[1:].lstrip()
    return s


def is_cmd(text: str, label: str) -> bool:
    """Match button text regardless of emoji或尾部追加的数字。"""
    candidate = (text or '').strip()
    if not candidate:
        return False
    label_full = (label or '').strip()
    label_plain = _strip_emoji_prefix(label_full)
    options = [label_full, label_plain]
    for target in options:
        if not target:
            continue
        if candidate.endswith(target) or candidate.startswith(target):
            return True
    return False


def extract_account_id(text: str) -> Optional[int]:
    if not text:
        return None
    m = re.search(r'(\d+)$', text.strip())
    return int(m.group(1)) if m else None


def split_keywords_payload(payload: str) -> list:
    if not payload:
        return []
    normalized = (
        payload.replace('，', ',')
        .replace('、', ',')
        .replace(';', ',')
    )
    result = []
    for part in re.split(r'[\n,]+', normalized):
        p = (part or '').strip()
        if p:
            result.append(p)
    return result


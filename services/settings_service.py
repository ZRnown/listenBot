import os
from typing import List, Optional, Tuple
from storage import dao_settings
from storage import dao_listen_sources

ADMIN_WHITELIST = set([int(x) for x in os.getenv('ADMIN_WHITELIST','').split(',') if x.strip().isdigit()])

TARGET_CHAT_KEY = 'target_chat'  # global scope
TARGET_BOT_KEY = 'target_bot'    # global scope: @username without '@'
GLOBAL_TEMPLATE_KEY = 'global_template'  # global scope: 发送消息模板
GLOBAL_SEND_DELAY_KEY = 'global_send_delay'  # global scope: 发送延迟
CLICK_DELAY_KEY = 'click_delay'
SEND_DELAY_KEY = 'send_delay'
CONCURRENCY_KEY = 'concurrency'
AUTO_JOIN_KEY = 'auto_join'
TEMPLATE_MSG_KEY = 'template_message'
START_SENDING_KEY = 'start_sending'
ACCOUNT_ROLE_KEY = 'role'  # account scope: 'listen' or 'click' or 'both'


def set_target_chat(target: str):
    dao_settings.set_setting('global', TARGET_CHAT_KEY, target)


def get_target_chat():
    return dao_settings.get_setting_value('global', TARGET_CHAT_KEY)


def set_target_bot(username: str):
    """保存目标机器人用户名（不带@）"""
    u = (username or '').strip().lstrip('@')
    if u.startswith('http://') or u.startswith('https://'):
        u = u.rsplit('/', 1)[-1].lstrip('@')
    dao_settings.set_setting('global', TARGET_BOT_KEY, u)


def get_target_bot() -> Optional[str]:
    return dao_settings.get_setting_value('global', TARGET_BOT_KEY)


def set_global_template(text: str):
    """设置全局发送消息模板"""
    dao_settings.set_setting('global', GLOBAL_TEMPLATE_KEY, text)


def get_global_template() -> Optional[str]:
    return dao_settings.get_setting_value('global', GLOBAL_TEMPLATE_KEY)


def set_global_send_delay(value: float):
    """设置全局发送延迟（秒）"""
    dao_settings.set_setting('global', GLOBAL_SEND_DELAY_KEY, str(value))


def get_global_send_delay() -> float:
    v = dao_settings.get_setting_value('global', GLOBAL_SEND_DELAY_KEY)
    try:
        return float(v) if v else 0.0
    except Exception:
        return 0.0


def set_account_target_chat(account_id: int, target: Optional[str]):
    # if target is empty, clear account-level and fallback to global
    if target is None or str(target).strip() == '':
        dao_settings.set_setting('account', TARGET_CHAT_KEY, None, account_id=account_id)
    else:
        dao_settings.set_setting('account', TARGET_CHAT_KEY, str(target), account_id=account_id)


def get_account_target_chat(account_id: int) -> Optional[str]:
    return dao_settings.get_setting_value('account', TARGET_CHAT_KEY, account_id)


def set_account_role(account_id: int, role: str):
    # expected values: 'listen', 'click', 'both'
    dao_settings.set_setting('account', ACCOUNT_ROLE_KEY, role, account_id=account_id)


def get_account_role(account_id: int) -> Optional[str]:
    return dao_settings.get_setting_value('account', ACCOUNT_ROLE_KEY, account_id)


def set_account_keywords(account_id: int, keywords, kind: str = 'listen'):
    from storage import dao_keywords
    dao_keywords.set_keywords(account_id, keywords, kind=kind)


def get_account_keywords(account_id: int, kind: str = 'listen'):
    from storage import dao_keywords
    return dao_keywords.get_keywords(account_id, kind=kind)


def add_keyword(account_id: int, word: str, kind: str = 'listen'):
    from storage import dao_keywords
    dao_keywords.add_keyword(account_id, word, kind=kind)


def delete_keyword(account_id: int, word: str, kind: str = 'listen'):
    from storage import dao_keywords
    dao_keywords.delete_keyword(account_id, word, kind=kind)


# ---- delays and concurrency helpers ----
def set_click_delay(value: str, account_id: Optional[int] = None):
    if account_id is None:
        dao_settings.set_setting('global', CLICK_DELAY_KEY, value)
    else:
        dao_settings.set_setting('account', CLICK_DELAY_KEY, value, account_id=account_id)


def get_click_delay(account_id: Optional[int] = None) -> float:
    # value stored as seconds (string), fallback to global then default 0
    if account_id is not None:
        v = dao_settings.get_setting_value('account', CLICK_DELAY_KEY, account_id)
        if v is not None:
            try:
                return float(v)
            except Exception:
                return 0.0
    v = dao_settings.get_setting_value('global', CLICK_DELAY_KEY)
    try:
        return float(v) if v is not None else 0.0
    except Exception:
        return 0.0


def set_send_delay(value: str, account_id: int):
    dao_settings.set_setting('account', SEND_DELAY_KEY, value, account_id=account_id)


def get_send_delay(account_id: int) -> float:
    v = dao_settings.get_setting_value('account', SEND_DELAY_KEY, account_id)
    try:
        return float(v) if v is not None else 0.0
    except Exception:
        return 0.0


def set_concurrency(value: int, account_id: int):
    dao_settings.set_setting('account', CONCURRENCY_KEY, str(int(value)), account_id=account_id)


def get_concurrency(account_id: int) -> int:
    v = dao_settings.get_setting_value('account', CONCURRENCY_KEY, account_id)
    try:
        return int(v) if v is not None else 1
    except Exception:
        return 1


def set_template_message(text: str, account_id: int):
    dao_settings.set_setting('account', TEMPLATE_MSG_KEY, text, account_id=account_id)


def get_template_message(account_id: int) -> Optional[str]:
    return dao_settings.get_setting_value('account', TEMPLATE_MSG_KEY, account_id)


def set_start_sending(enabled: bool, account_id: int):
    dao_settings.set_setting('account', START_SENDING_KEY, '1' if enabled else '0', account_id=account_id)


def get_start_sending(account_id: int) -> bool:
    v = dao_settings.get_setting_value('account', START_SENDING_KEY, account_id)
    return str(v) == '1'


# ---- listen sources (whitelist for listen role) ----
def get_listen_sources(account_id: int) -> List[str]:
    return dao_listen_sources.list_sources(account_id)


def add_listen_source(account_id: int, source: str):
    dao_listen_sources.add_source(account_id, source)


def bulk_add_listen_sources(account_id: int, sources: List[str]):
    dao_listen_sources.bulk_add(account_id, sources)


def delete_listen_source(account_id: int, source: str):
    dao_listen_sources.delete_source(account_id, source)


def clear_listen_sources(account_id: int):
    dao_listen_sources.clear_sources(account_id)


# ---- anti-ban defaults (simple getters; later can be settable) ----
def get_join_delay_range() -> Tuple[float, float]:
    # default 0.8 ~ 2.2 seconds
    return (0.8, 2.2)


def get_click_jitter() -> float:
    # +- jitter seconds to add on top of click delay
    return 0.3


def get_send_jitter() -> float:
    # +- jitter seconds to add on top of send delay
    return 0.3


def clear_account_settings(account_id: int):
    dao_settings.delete_account_settings(account_id)

from services import settings_service


def match_keywords(account_id: int, text: str, kind: str = 'listen'):
    if not text:
        return None
    kws = settings_service.get_account_keywords(account_id, kind=kind) or []
    for k in kws:
        if k and k in text:
            return k
    return None

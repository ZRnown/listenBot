from services import settings_service


def match_keywords(account_id: int, text: str, kind: str = 'listen'):
    if not text:
        return None
    kws = settings_service.get_account_keywords(account_id, kind=kind) or []
    for k in kws:
        if k and k.strip():
            # 使用 strip() 去除关键词两端的空格
            keyword = k.strip()
            # 检查关键词是否在文本中（大小写敏感）
            if keyword in text:
                return keyword
    return None

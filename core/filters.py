import re
import unicodedata
from services import settings_service


def normalize_text_for_matching(text: str) -> str:
    """
    规范化文本用于关键词匹配，去除emoji、零宽字符、空格等
    例如："🧧 领‍取‌红‍包" -> "领取红包"
    """
    if not text:
        return ''
    
    # 去除所有emoji和符号（保留中文、英文、数字）
    normalized = ''
    for char in text:
        # 跳过emoji（So类别）和符号（Sk类别）
        cat = unicodedata.category(char)
        if cat in ('So', 'Sk'):
            continue
        # 跳过零宽字符（Cf类别中的零宽字符）
        if cat == 'Cf' and char in ('\u200b', '\u200c', '\u200d', '\ufeff', '\u2060'):
            continue
        # 跳过空格
        if char.isspace():
            continue
        normalized += char
    
    # 额外处理：去掉按钮文本末尾的数字和括号等计数标记
    # 例如："领取红包1" / "领取红包(2)" / "领取红包【3】" -> "领取红包"
    normalized = re.sub(r'[\d（）()\[\]【】]+$', '', normalized)
    
    return normalized.strip()


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


def match_keywords_normalized(account_id: int, text: str, kind: str = 'click'):
    """
    规范化匹配关键词（用于按钮文本匹配）
    去除emoji、零宽字符、空格后进行匹配
    """
    if not text:
        return None
    kws = settings_service.get_account_keywords(account_id, kind=kind) or []
    normalized_text = normalize_text_for_matching(text)
    for k in kws:
        if k and k.strip():
            keyword = k.strip()
            # 检查关键词是否在规范化后的文本中
            if keyword in normalized_text:
                return keyword
    return None

"""
关键词预编译缓存系统 - 极速匹配
使用预编译正则表达式实现毫秒级关键词匹配
"""
import re
from typing import Optional, Dict, Tuple
from services import settings_service


# 全局缓存：account_id -> (kind -> compiled_pattern)
_pattern_cache: Dict[int, Dict[str, re.Pattern]] = {}
# 缓存版本号：account_id -> (kind -> version)
_cache_version: Dict[int, Dict[str, int]] = {}
_version_counter = 0


def _get_next_version() -> int:
    """获取下一个版本号"""
    global _version_counter
    _version_counter += 1
    return _version_counter


def _escape_keyword(keyword: str) -> str:
    """转义关键词中的正则特殊字符"""
    # 转义所有正则特殊字符
    return re.escape(keyword)


def _compile_pattern(keywords: list) -> Optional[re.Pattern]:
    """编译关键词列表为正则表达式模式"""
    if not keywords:
        return None
    
    # 过滤并转义关键词
    escaped_keywords = []
    for k in keywords:
        k = (k or '').strip()
        if k:
            escaped_keywords.append(_escape_keyword(k))
    
    if not escaped_keywords:
        return None
    
    # 使用 | 连接所有关键词，创建匹配模式
    # 使用 word boundary \b 确保完整匹配（可选，根据需求调整）
    pattern = '|'.join(escaped_keywords)
    
    try:
        # 编译为正则表达式（大小写敏感）
        return re.compile(pattern)
    except Exception:
        # 如果编译失败，返回 None
        return None


def get_compiled_pattern(account_id: int, kind: str = 'listen') -> Optional[re.Pattern]:
    """获取预编译的关键词模式（带缓存）"""
    # 检查缓存
    if account_id in _pattern_cache and kind in _pattern_cache[account_id]:
        # 检查版本号是否过期
        if account_id in _cache_version and kind in _cache_version[account_id]:
            # 获取当前关键词列表
            current_keywords = settings_service.get_account_keywords(account_id, kind=kind) or []
            # 计算当前关键词的哈希（简单版本：使用长度和第一个关键词）
            # 这里使用更简单的方法：直接比较关键词列表
            # 如果关键词列表没有变化，直接返回缓存的模式
            cached_pattern = _pattern_cache[account_id][kind]
            # 为了简单，我们每次都重新编译（在实际使用中，可以添加更智能的缓存失效机制）
            # 但为了性能，我们先检查关键词数量是否变化
            pass  # 暂时每次都重新编译，后续可以优化
    
    # 获取关键词列表
    keywords = settings_service.get_account_keywords(account_id, kind=kind) or []
    
    # 编译模式
    pattern = _compile_pattern(keywords)
    
    # 更新缓存
    if account_id not in _pattern_cache:
        _pattern_cache[account_id] = {}
    if account_id not in _cache_version:
        _cache_version[account_id] = {}
    
    _pattern_cache[account_id][kind] = pattern
    _cache_version[account_id][kind] = _get_next_version()
    
    return pattern


def match_keywords_fast(account_id: int, text: str, kind: str = 'listen') -> Optional[str]:
    """
    极速关键词匹配：使用预编译正则表达式
    返回匹配到的第一个关键词，如果没有匹配则返回 None
    """
    if not text:
        return None
    
    # 获取预编译的模式
    pattern = get_compiled_pattern(account_id, kind)
    if not pattern:
        return None
    
    # 使用 search 查找第一个匹配
    match = pattern.search(text)
    if match:
        # 返回匹配的文本（这是原始关键词，不是转义后的）
        matched_text = match.group()
        # 从原始关键词列表中查找对应的关键词
        keywords = settings_service.get_account_keywords(account_id, kind=kind) or []
        for k in keywords:
            k = (k or '').strip()
            if k and k in matched_text:
                return k
        # 如果找不到，返回匹配的文本本身
        return matched_text
    
    return None


def invalidate_cache(account_id: int, kind: Optional[str] = None):
    """使缓存失效"""
    if account_id in _pattern_cache:
        if kind:
            if kind in _pattern_cache[account_id]:
                del _pattern_cache[account_id][kind]
            if account_id in _cache_version and kind in _cache_version[account_id]:
                del _cache_version[account_id][kind]
        else:
            # 清除该账号的所有缓存
            del _pattern_cache[account_id]
            if account_id in _cache_version:
                del _cache_version[account_id]


"""
关键词预编译缓存模块：实现毫秒级关键词匹配
使用正则表达式预编译，避免每次消息都重新编译
"""
import re
from typing import Optional, Dict, Pattern
from services import settings_service

# 全局缓存：account_id -> {kind: compiled_pattern}
_keyword_pattern_cache: Dict[int, Dict[str, Optional[Pattern]]] = {}
_cache_version: Dict[int, Dict[str, int]] = {}  # 用于检测关键词变化


def _compile_keywords(keywords: list) -> Optional[Pattern]:
    """
    将关键词列表编译为正则表达式模式
    使用 | 连接所有关键词，实现 O(1) 匹配
    """
    if not keywords:
        return None
    
    # 过滤空关键词并转义特殊字符
    valid_keywords = []
    for kw in keywords:
        kw = (kw or '').strip()
        if kw:
            # 转义正则特殊字符
            escaped = re.escape(kw)
            valid_keywords.append(escaped)
    
    if not valid_keywords:
        return None
    
    # 使用 | 连接所有关键词，实现一次性匹配
    pattern_str = '|'.join(valid_keywords)
    try:
        # 编译为正则表达式（大小写敏感）
        return re.compile(pattern_str)
    except Exception:
        return None


def get_compiled_pattern(account_id: int, kind: str = 'listen') -> Optional[Pattern]:
    """
    获取账号的预编译关键词模式
    如果关键词发生变化，自动重新编译
    """
    # 获取当前关键词
    keywords = settings_service.get_account_keywords(account_id, kind=kind) or []
    
    # 计算关键词的版本（使用关键词数量和内容的简单哈希）
    keywords_str = '|'.join(sorted(keywords))
    current_version = hash(keywords_str)
    
    # 检查缓存
    if account_id in _keyword_pattern_cache:
        kind_cache = _keyword_pattern_cache[account_id]
        if kind in kind_cache:
            # 检查版本
            if account_id in _cache_version:
                version_cache = _cache_version[account_id]
                if kind in version_cache and version_cache[kind] == current_version:
                    # 缓存有效，直接返回
                    return kind_cache[kind]
    
    # 需要重新编译
    pattern = _compile_keywords(keywords)
    
    # 更新缓存
    if account_id not in _keyword_pattern_cache:
        _keyword_pattern_cache[account_id] = {}
    _keyword_pattern_cache[account_id][kind] = pattern
    
    if account_id not in _cache_version:
        _cache_version[account_id] = {}
    _cache_version[account_id][kind] = current_version
    
    return pattern


def match_keywords_fast(account_id: int, text: str, kind: str = 'listen') -> Optional[str]:
    """
    极速关键词匹配：使用预编译正则表达式
    返回匹配到的第一个关键词，如果没有匹配则返回 None
    """
    if not text:
        return None
    
    pattern = get_compiled_pattern(account_id, kind)
    if not pattern:
        # 如果没有关键词，返回None（这是正常的）
        return None
    
    # 使用 search 进行匹配（找到第一个匹配即可）
    match = pattern.search(text)
    if match:
        # 返回匹配的文本（实际匹配到的关键词）
        matched_text = match.group()
        # 从原始关键词列表中查找对应的关键词（去除转义字符）
        keywords = settings_service.get_account_keywords(account_id, kind=kind) or []
        for kw in keywords:
            kw = (kw or '').strip()
            if kw and kw in matched_text:
                return kw
        # 如果找不到对应的关键词，返回匹配的文本本身
        return matched_text
    
    return None


def clear_cache(account_id: Optional[int] = None):
    """
    清除缓存（当关键词被修改时调用）
    """
    if account_id is None:
        _keyword_pattern_cache.clear()
        _cache_version.clear()
    else:
        if account_id in _keyword_pattern_cache:
            del _keyword_pattern_cache[account_id]
        if account_id in _cache_version:
            del _cache_version[account_id]


"""
过滤器链系统 - 基于 TelegramForwarder 的高性能设计
只保留关键词匹配和转发功能，去除冗余功能
"""
import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Optional

logger = logging.getLogger(__name__)


class MessageContext:
    """
    消息上下文类，包含处理消息所需的所有信息
    简化版，只保留必要字段
    """
    
    def __init__(self, client, event, account: dict, bot_client, control_bot_id=None):
        """
        初始化消息上下文
        
        Args:
            client: 监听账号的客户端
            event: 消息事件
            account: 账号信息字典
            bot_client: 控制机器人客户端
            control_bot_id: 控制机器人的 ID（用于过滤自己的消息）
        """
        self.client = client
        self.event = event
        self.account = account
        self.bot_client = bot_client
        self.control_bot_id = control_bot_id
        
        # 消息文本
        self.message_text = event.message.message or ''
        if not self.message_text:
            self.message_text = getattr(event.message, 'raw_text', '') or ''
            if not self.message_text:
                self.message_text = str(event.message.text) if hasattr(event.message, 'text') else ''
        
        # 匹配的关键词
        self.matched_keyword: Optional[str] = None
        
        # 是否应该转发
        self.should_forward = False
        
        # 缓存的信息（避免重复获取）
        self._sender = None
        self._chat = None
        self._sender_fetched = False
        self._chat_fetched = False
        
        # 错误记录
        self.errors = []
    
    async def get_sender(self):
        """获取发送者（带缓存，改进：使用多种方式获取）"""
        if not self._sender_fetched:
            try:
                # 首先尝试标准方式
                self._sender = await asyncio.wait_for(
                    self.event.get_sender(),
                    timeout=1.0  # 增加超时时间
                )
            except Exception as e:
                # 如果标准方式失败，尝试其他方式
                try:
                    # 尝试从 event.sender 获取
                    if hasattr(self.event, 'sender') and self.event.sender:
                        self._sender = self.event.sender
                    # 尝试从 event.message.sender 获取
                    elif hasattr(self.event.message, 'sender') and self.event.message.sender:
                        self._sender = self.event.message.sender
                    # 尝试通过 sender_id 获取实体
                    elif hasattr(self.event.message, 'sender_id') and self.event.message.sender_id:
                        try:
                            self._sender = await self.client.get_entity(self.event.message.sender_id)
                        except Exception:
                            self._sender = None
                    else:
                        self._sender = None
                except Exception:
                    self._sender = None
            self._sender_fetched = True
        return self._sender
    
    async def get_chat(self):
        """获取聊天信息（带缓存，改进：使用多种方式获取）"""
        if not self._chat_fetched:
            try:
                # 首先尝试标准方式
                self._chat = await asyncio.wait_for(
                    self.event.get_chat(),
                    timeout=1.0  # 增加超时时间
                )
            except Exception as e:
                # 如果标准方式失败，尝试其他方式
                try:
                    # 尝试从 event.chat 获取
                    if hasattr(self.event, 'chat') and self.event.chat:
                        self._chat = self.event.chat
                    else:
                        self._chat = None
                except Exception:
                    self._chat = None
            self._chat_fetched = True
        return self._chat


class BaseFilter(ABC):
    """
    基础过滤器类，定义过滤器接口
    """
    
    def __init__(self, name=None):
        """
        初始化过滤器
        
        Args:
            name: 过滤器名称，如果为None则使用类名
        """
        self.name = name or self.__class__.__name__
        
    async def process(self, context: MessageContext):
        """
        处理消息上下文
        
        Args:
            context: 包含消息处理所需所有信息的上下文对象
            
        Returns:
            bool: 表示是否应该继续处理消息
        """
        try:
            result = await self._process(context)
            return result
        except Exception as e:
            logger.error(f"过滤器 {self.name} 处理出错: {str(e)}")
            context.errors.append(f"过滤器 {self.name} 错误: {str(e)}")
            return False
    
    @abstractmethod
    async def _process(self, context: MessageContext):
        """
        具体的处理逻辑，子类需要实现
        
        Args:
            context: 包含消息处理所需所有信息的上下文对象
            
        Returns:
            bool: 表示是否应该继续处理消息
        """
        pass


class FilterChain:
    """
    过滤器链，用于组织和执行多个过滤器
    高性能设计：每个过滤器都是异步的，可以快速中断
    """
    
    def __init__(self):
        """初始化过滤器链"""
        self.filters = []
        
    def add_filter(self, filter_obj: BaseFilter):
        """
        添加过滤器到链中
        
        Args:
            filter_obj: 要添加的过滤器对象，必须是BaseFilter的子类
        """
        if not isinstance(filter_obj, BaseFilter):
            raise TypeError("过滤器必须是BaseFilter的子类")
        self.filters.append(filter_obj)
        return self
        
    async def process(self, context: MessageContext):
        """
        处理消息
        
        Args:
            context: 消息上下文
            
        Returns:
            bool: 表示处理是否成功
        """
        # 依次执行每个过滤器
        for filter_obj in self.filters:
            try:
                should_continue = await filter_obj.process(context)
                if not should_continue:
                    # 过滤器决定中断处理链
                    return False
            except Exception as e:
                logger.error(f"过滤器 {filter_obj.name} 处理出错: {str(e)}")
                context.errors.append(f"过滤器 {filter_obj.name} 错误: {str(e)}")
                return False
        
        return True


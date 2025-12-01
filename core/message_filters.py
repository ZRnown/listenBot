"""
消息过滤器实现 - 关键词匹配和转发
基于 TelegramForwarder 的高性能设计
"""
import asyncio
import logging
from datetime import datetime
from core.filter_chain import BaseFilter, MessageContext
from core.filters import match_keywords
from services import settings_service
from services.alerting import send_alert

logger = logging.getLogger(__name__)


class KeywordFilter(BaseFilter):
    """
    关键词过滤器，检查消息是否包含指定关键词
    高性能：快速匹配，不阻塞
    """
    
    async def _process(self, context: MessageContext):
        """
        检查消息是否包含关键词
        
        Args:
            context: 消息上下文
            
        Returns:
            bool: 若消息应继续处理则返回True，否则返回False
        """
        account_id = context.account['id']
        msg_text = context.message_text
        msg_id = getattr(context.event.message, 'id', None)
        chat_id = getattr(context.event, 'chat_id', None)
        
        print(f"[KeywordFilter] 账号 #{account_id} 开始检查关键词: Chat ID={chat_id}, Msg ID={msg_id}, 文本长度={len(msg_text)}")
        
        role = settings_service.get_account_role(account_id) or 'both'
        
        # 只处理监听账号
        if role not in ('listen', 'both'):
            print(f"[KeywordFilter] 账号 #{account_id} 不是监听账号，跳过")
            return False
        
        # 快速匹配关键词
        keywords = settings_service.get_account_keywords(account_id, kind='listen') or []
        print(f"[KeywordFilter] 账号 #{account_id} 关键词数量: {len(keywords)}")
        if not keywords:
            print(f"[KeywordFilter] 账号 #{account_id} 没有关键词，跳过")
            return False
        
        # 详细日志：打印关键词列表和消息文本
        print(f"[KeywordFilter] 账号 #{account_id} 关键词列表: {keywords}")
        print(f"[KeywordFilter] 账号 #{account_id} 消息文本: '{msg_text}'")
        
        matched = match_keywords(account_id, msg_text, kind='listen')
        print(f"[KeywordFilter] 账号 #{account_id} 关键词匹配结果: {matched}")
        
        # 如果没有匹配，尝试详细匹配每个关键词
        if not matched:
            print(f"[KeywordFilter] 账号 #{account_id} 开始逐个检查关键词:")
            for kw in keywords:
                if kw and kw.strip():
                    keyword = kw.strip()
                    is_match = keyword in msg_text
                    print(f"[KeywordFilter]   关键词 '{keyword}' 在文本 '{msg_text}' 中: {is_match}")
        
        if matched:
            context.matched_keyword = matched
            context.should_forward = True
            
            # 快速日志
            timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
            logger.info(f"[监听] [{timestamp}] ✅ 账号 #{account_id} 匹配关键词: '{matched}' (消息ID: {msg_id}, Chat ID: {chat_id})")
            print(f"[KeywordFilter] ✅ 账号 #{account_id} 匹配关键词: '{matched}'")
            return True
        
        print(f"[KeywordFilter] 账号 #{account_id} 未匹配到关键词")
        return False


class SenderFilter(BaseFilter):
    """
    发送过滤器，负责发送提醒消息
    高性能：完全异步，不阻塞
    保持用户现有的转发格式
    """
    
    async def _process(self, context: MessageContext):
        """
        发送提醒消息
        
        Args:
            context: 消息上下文
            
        Returns:
            bool: 是否继续处理
        """
        if not context.should_forward or not context.matched_keyword:
            return False
        
        # 检查转发目标
        target = settings_service.get_target_chat()
        if not target or not target.strip() or not context.bot_client:
            timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
            logger.warning(f"[监听] [{timestamp}] ⚠️ 转发目标未设置或 bot_client 为空，跳过发送")
            return False
        
        # 极致优化：立即发送提醒，不等待任何检查，真正并发
        # send_alert 内部已经使用队列和快速检查，不会阻塞
        # 每个匹配的消息都立即创建独立任务，不受其他消息影响
        async def _send_alert_task():
            try:
                # 快速检查发送者（不阻塞主流程）
                sender = await context.get_sender()
                if sender:
                    sender_id = getattr(sender, 'id', None)
                    is_bot = getattr(sender, 'bot', False)
                    if is_bot and context.control_bot_id and sender_id == context.control_bot_id:
                        logger.info(f"[监听] ⚠️ 消息来自控制机器人本身（ID: {sender_id}），跳过发送")
                        return
                
                # 立即发送提醒，完全异步，不阻塞
                # send_alert 内部已经使用队列，不会阻塞
                await send_alert(
                    context.bot_client,
                    context.account,
                    context.event,
                    context.matched_keyword,
                    control_bot_id=context.control_bot_id
                )
            except Exception as e:
                error_timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
                logger.error(f"[监听] [{error_timestamp}] ❌ 发送提醒失败 (账号 #{context.account['id']}): {str(e)}")
        
        # 立即创建任务，不等待完成，真正并发
        # 每个消息匹配都立即创建独立任务，不受其他任务影响
        asyncio.create_task(_send_alert_task())
        
        return True


class TemplateSenderFilter(BaseFilter):
    """
    模板消息发送过滤器（如果启用）
    高性能：完全异步，不阻塞
    """
    
    async def _process(self, context: MessageContext):
        """
        发送模板消息（如果启用）
        
        Args:
            context: 消息上下文
            
        Returns:
            bool: 是否继续处理
        """
        if not context.should_forward or not context.matched_keyword:
            return True  # 不中断处理链
        
        account_id = context.account['id']
        
        # 检查是否启用自动发送模板
        if not settings_service.get_start_sending(account_id):
            return True
        
        # 获取模板
        tpl = settings_service.get_template_message(account_id)
        if not tpl:
            return True
        
        # 异步发送模板消息
        async def _send_template():
            try:
                await context.client.send_message(
                    context.event.chat_id,
                    tpl
                )
            except Exception as e:
                logger.error(f"[模板] ❌ 账号 #{account_id} 发送模板失败: {str(e)}")
        
        # 立即创建任务，不等待完成
        asyncio.create_task(_send_template())
        
        return True


import asyncio
import os
import random
import re
import unicodedata
from typing import List, Optional
from telethon import events, TelegramClient
from telethon.tl.custom import Button
from bot.keyboards import main_keyboard, roles_keyboard
from services import settings_service
from services import joining
from storage import dao_accounts
from storage import dao_keywords
from services import sessions as sess_service
from core.clients import ClientManager
from core.filters import normalize_text_for_matching

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


def split_keywords_payload(payload: str) -> List[str]:
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


def get_account_role(account_id: int) -> str:
    return settings_service.get_account_role(account_id) or 'both'


def role_allows_listen(role: str) -> bool:
    return role in ('listen', 'both')


def role_allows_click(role: str) -> bool:
    return role in ('click', 'both')


def format_role_label(role: str) -> str:
    return {'listen': '监听', 'click': '点击', 'both': '监听+点击'}.get(role, role)


def account_summary_text(row) -> str:
    acc_id = row['id']
    role = get_account_role(acc_id)
    ident = row['username'] or row['phone'] or ''
    status = row['status']
    start_flag = '开启' if settings_service.get_start_sending(acc_id) else '关闭'
    listen_kw = len(settings_service.get_account_keywords(acc_id, kind='listen') or [])
    click_kw = len(settings_service.get_account_keywords(acc_id, kind='click') or [])
    lines = [
        f"#{acc_id} | {ident or '无用户名'} | {status}",
        f"角色：{format_role_label(role)}  ▶️ 发送：{start_flag}",
    ]
    if role_allows_listen(role):
        lines.append(f"监听关键字：{listen_kw} 条")
    if role_allows_click(role):
        lines.append(f"点击关键字：{click_kw} 条")
    return '\n'.join(lines)


def account_base_buttons(acc_id: int):
    return [
        [Button.inline('⚙️ 设置', data=f'acc|{acc_id}|menu'), Button.inline('🗑️ 删除', data=f'acc|{acc_id}|delete')]
    ]


def account_menu_buttons(acc_id: int):
    role = get_account_role(acc_id)
    buttons = []
    if role_allows_listen(role):
        buttons.append([Button.inline('监听关键字', data=f'acc|{acc_id}|kwl')])
    if role_allows_click(role):
        buttons.append([Button.inline('点击关键字', data=f'acc|{acc_id}|kwc')])
        buttons.append([Button.inline('📝 模板', data=f'acc|{acc_id}|tmpl'),
                        Button.inline('🐢 发送延迟', data=f'acc|{acc_id}|delay')])
        buttons.append([Button.inline('⏱️ 点击延迟', data=f'acc|{acc_id}|clickdelay')])
        start_label = '⏸️ 停止发送' if settings_service.get_start_sending(acc_id) else '▶️ 开始发送'
        buttons.append([Button.inline(start_label, data=f'acc|{acc_id}|start')])
    buttons.append([Button.inline('⬅️ 返回', data=f'acc|{acc_id}|back')])
    return buttons


def account_menu_text(row) -> str:
    role = get_account_role(row['id'])
    return (
        f"#{row['id']} 操作面板（{format_role_label(role)}）\n"
        "请选择要执行的操作："
    )


def list_accounts(role_filter: Optional[str] = None):
    rows = dao_accounts.list_all()
    if role_filter == 'click':
        return [r for r in rows if role_allows_click(get_account_role(r['id']))]
    if role_filter == 'listen':
        return [r for r in rows if role_allows_listen(get_account_role(r['id']))]
    return rows


async def parse_and_execute_click(manager: ClientManager, link_text: str, report_chat_id: int):
    """解析链接并执行点击任务（自动识别链接）"""
    try:
        # 解析消息链接
        chat_id_from_link = None
        msg_id_from_link = None
        
        print(f"[自动点击] 开始解析链接: {link_text}")
        
        # 解析 t.me/c/xxx/123 格式（超级群组/频道）
        try:
            match1 = re.search(r't\.me/c/(\d+)/(\d+)', link_text)
            if match1:
                channel_id = match1.group(1)
                msg_id_from_link = int(match1.group(2))
                chat_id_from_link = int(f'-100{channel_id}')
                print(f"[自动点击] 解析成功（频道格式）: Chat ID={chat_id_from_link}, Message ID={msg_id_from_link}")
            else:
                # 解析 t.me/username/123 格式
                match2 = re.search(r't\.me/([a-zA-Z0-9_]+)/(\d+)', link_text)
                if match2:
                    username = match2.group(1)
                    msg_id_from_link = int(match2.group(2))
                    chat_id_from_link = username
                    print(f"[自动点击] 解析成功（用户名格式）: Chat ID={chat_id_from_link}, Message ID={msg_id_from_link}")
        except Exception as parse_error:
            print(f"[自动点击] ❌ 解析链接时出错: {parse_error}")
            return False, f'解析链接时出错：{parse_error}'
        
        # 验证链接格式
        if not chat_id_from_link or not msg_id_from_link:
            print(f"[自动点击] ❌ 链接解析失败 - chat_id或msg_id为空")
            return False, '消息链接格式无效'
        
        # 获取所有点击账号
        try:
            click_accounts = list_accounts('click')
            print(f"[自动点击] 找到 {len(click_accounts)} 个点击账号")
        except Exception as list_error:
            print(f"[自动点击] ❌ 获取账号列表失败: {list_error}")
            return False, f'获取账号列表失败：{list_error}'
        
        if not click_accounts:
            print(f"[自动点击] ⚠️ 没有可用的点击账号")
            return False, '没有可用的点击账号'
        
        # 异步执行点击任务（不阻塞）
        print(f"[自动点击] 🚀 创建异步任务: Chat ID={chat_id_from_link}, Message ID={msg_id_from_link}, 账号数={len(click_accounts)}")
        
        async def safe_start_click_job():
            """安全包装的点击任务，确保所有异常都被捕获并反馈"""
            try:
                await start_click_job(
                    manager, chat_id_from_link, msg_id_from_link, click_accounts, report_chat_id
                )
            except Exception as e:
                print(f"[自动点击] ❌ 任务执行异常: {e}")
                import traceback
                traceback.print_exc()
                try:
                    error_msg = (
                        f'❌ **点击任务执行失败**\n'
                        f'━━━━━━━━━━━━━━━━\n'
                        f'错误信息：`{str(e)}`\n\n'
                        f'请检查：\n'
                        f'• 账号是否在线\n'
                        f'• 消息链接是否正确\n'
                        f'• 账号是否已加入目标群组'
                    )
                    await manager.bot.send_message(
                        report_chat_id, 
                        error_msg, 
                        parse_mode='markdown',
                        buttons=main_keyboard()
                    )
                except Exception as send_error:
                    print(f"[自动点击] ❌ 发送错误消息失败: {send_error}")
        
        # 创建并立即调度任务
        asyncio.create_task(safe_start_click_job())
        return True, None
    
    except Exception as e:
        print(f"[自动点击] ❌ 处理过程中出现未捕获的异常: {e}")
        import traceback
        traceback.print_exc()
        return False, f'处理失败：{e}'


async def start_click_job(manager: ClientManager, target_chat_id, target_msg_id, accounts: List[dict], report_chat_id: int):
    """开始点击任务：获取消息、匹配关键词并并发点击（控制并发数避免封号）"""
    bot = manager.bot
    if not bot:
        print(f"[点击任务] ❌ 控制机器人未初始化")
        return
    
    print(f"[点击任务] 🚀 开始点击任务: Chat ID={target_chat_id}, Message ID={target_msg_id}, 账号数={len(accounts)}")
    try:
        # 使用第一个可用的账号客户端获取消息
        target_msg = None
        buttons = None
        button_positions = []
        error_details = []  # 记录所有尝试的错误信息
        
        print(f"[点击任务] 开始尝试获取消息，共有 {len(accounts)} 个账号")
        print(f"[点击任务] 当前在线账号数: {len(manager.account_clients)}")
        print(f"[点击任务] 在线账号ID列表: {list(manager.account_clients.keys())}")
        
        for acc in accounts:
            acc_id = acc['id']
            acc_name = acc.get('username') or acc.get('phone') or f"#{acc_id}"
            client = manager.account_clients.get(acc_id)
            if not client:
                print(f"[点击任务] ⚠️ 账号 {acc_name} (#{acc_id}) 客户端不存在")
                error_details.append(f"账号 {acc_name} (#{acc_id}): 客户端不存在")
                continue
            
            # 检查客户端是否真正连接
            try:
                if not client.is_connected():
                    print(f"[点击任务] ⚠️ 账号 {acc_name} (#{acc_id}) 客户端未连接")
                    error_details.append(f"账号 {acc_name} (#{acc_id}): 客户端未连接")
                    continue
            except Exception as conn_check_error:
                print(f"[点击任务] ⚠️ 账号 {acc_name} (#{acc_id}) 检查连接状态失败: {conn_check_error}")
                error_details.append(f"账号 {acc_name} (#{acc_id}): 连接状态检查失败")
                continue
            
            try:
                print(f"[点击任务] 尝试使用账号 {acc_name} (#{acc_id}) 获取消息...")
                target_msg = await client.get_messages(target_chat_id, ids=target_msg_id)
                if target_msg:
                    buttons = getattr(target_msg, 'buttons', None)
                    if buttons:
                        for i, row in enumerate(buttons):
                            for j, btn in enumerate(row):
                                btn_text = getattr(btn, 'text', None) or ''
                                button_positions.append((i, j, btn_text))
                    print(f"[点击任务] ✅ 账号 {acc_name} (#{acc_id}) 成功获取消息，找到 {len(button_positions)} 个按钮")
                    break
                else:
                    print(f"[点击任务] ⚠️ 账号 {acc_name} (#{acc_id}) 获取的消息为空")
            except Exception as e:
                error_str = str(e)
                # 判断错误类型
                if 'CHANNEL_PRIVATE' in error_str or 'CHAT_FORBIDDEN' in error_str or 'USER_BANNED_IN_CHANNEL' in error_str:
                    error_details.append(f"账号 {acc_name} (#{acc_id}): 未加入该群组/频道或已被禁止")
                elif 'MESSAGE_NOT_FOUND' in error_str or 'MSG_ID_INVALID' in error_str:
                    error_details.append(f"账号 {acc_name} (#{acc_id}): 消息不存在或无效")
                else:
                    error_details.append(f"账号 {acc_name} (#{acc_id}): {error_str}")
                continue
        
        if not target_msg:
            error_msg = (
                f'❌ **无法获取消息**\n'
                f'━━━━━━━━━━━━━━━━\n'
                f'📋 消息链接：Chat ID: `{target_chat_id}`, Message ID: `{target_msg_id}`\n\n'
                f'**尝试了 {len(accounts)} 个账号，全部失败：**\n'
            )
            if error_details:
                for i, detail in enumerate(error_details[:10], 1):  # 最多显示10个错误
                    error_msg += f'{i}. {detail}\n'
                if len(error_details) > 10:
                    error_msg += f'... 还有 {len(error_details) - 10} 个账号失败\n'
            else:
                error_msg += '（无可用账号客户端）\n'
            
            error_msg += (
                f'\n**可能的原因：**\n'
                f'1. ⚠️ **所有账号都未加入该群组/频道**（最常见）\n'
                f'2. 消息链接无效或消息已被删除\n'
                f'3. 账号没有访问该消息的权限\n'
                f'4. 账号已被群组/频道管理员禁止\n\n'
                f'💡 **解决方案：**\n'
                f'• 确保至少有一个点击账号已加入目标群组/频道\n'
                f'• 检查消息链接是否正确\n'
                f'• 使用"🚪 自动进群"功能让账号加入群组'
            )
            try:
                await bot.send_message(report_chat_id, error_msg, parse_mode='markdown')
            except Exception as send_error:
                print(f"[点击任务] ❌ 发送错误消息失败: {send_error}")
            return
        
        if not buttons or not button_positions:
            try:
                await bot.send_message(report_chat_id, '⚠️ 该消息没有按钮')
            except Exception as send_error:
                print(f"[点击任务] ❌ 发送消息失败: {send_error}")
            return
        
        # 检查哪些账号有关键词匹配
        matched_accounts = []
        print(f"[点击任务] 开始匹配关键词，按钮数量: {len(button_positions)}")
        for acc in accounts:
            acc_id = acc['id']
            keywords = settings_service.get_account_keywords(acc_id, kind='click') or []
            print(f"[点击任务] 账号 #{acc_id} 的点击关键词: {keywords}")
            if not keywords:
                print(f"[点击任务] ⚠️ 账号 #{acc_id} 没有设置点击关键词")
                continue
            for i, j, btn_text in button_positions:
                matched_kw = None
                # 规范化按钮文本（去除emoji、零宽字符、空格）
                normalized_btn_text = normalize_text_for_matching(btn_text)
                print(f"[点击任务] 按钮文本: '{btn_text}' -> 规范化后: '{normalized_btn_text}'")
                for k in keywords:
                    if not k:
                        continue
                    # 规范化关键词（去除空格）
                    normalized_keyword = k.strip()
                    # 检查关键词是否在规范化后的按钮文本中
                    if normalized_keyword and normalized_keyword in normalized_btn_text:
                        matched_kw = k
                        break
                if matched_kw:
                    print(f"[点击任务] ✅ 账号 #{acc_id} 匹配到按钮 '{btn_text}' (关键词: {matched_kw})")
                    matched_accounts.append((acc, i, j, btn_text))
                    break
        
        if not matched_accounts:
            all_btn_texts = [bt[2] for bt in button_positions]
            print(f"[点击任务] ⚠️ 没有账号的关键词匹配到按钮")
            print(f"[点击任务] 按钮文本列表: {all_btn_texts}")
            print(f"[点击任务] 检查所有账号的关键词...")
            
            # 显示所有账号的关键词，帮助用户调试
            keywords_info = []
            no_keywords_accounts = []
            for acc in accounts:
                acc_id = acc['id']
                acc_name = acc.get('username') or acc.get('phone') or f"#{acc_id}"
                keywords = settings_service.get_account_keywords(acc_id, kind='click') or []
                if keywords:
                    keywords_info.append(f"账号 {acc_name}: {', '.join(keywords[:5])}")
                else:
                    no_keywords_accounts.append(acc_name)
            
            error_msg = (
                f'⚠️ **没有账号的关键词匹配到按钮**\n'
                f'━━━━━━━━━━━━━━━━\n'
                f'📋 按钮文本：{", ".join(all_btn_texts[:5])}{"..." if len(all_btn_texts) > 5 else ""}\n\n'
            )
            
            if no_keywords_accounts:
                error_msg += f'**未设置点击关键词的账号：**\n'
                for acc_name in no_keywords_accounts:
                    error_msg += f'• {acc_name}\n'
                error_msg += '\n'
            
            if keywords_info:
                error_msg += f'**当前点击关键词：**\n'
                for info in keywords_info[:10]:
                    error_msg += f'• {info}\n'
                error_msg += '\n'
            
            error_msg += (
                f'💡 **提示：**\n'
                f'• 检查按钮文本是否包含您设置的关键词\n'
                f'• 关键词匹配是大小写敏感的\n'
                f'• 可以在账号设置中添加或修改点击关键词\n'
                f'• 未设置关键词的账号不会参与点击'
            )
            
            try:
                await bot.send_message(report_chat_id, error_msg, parse_mode='markdown')
            except Exception as send_error:
                print(f"[点击任务] ❌ 发送消息失败: {send_error}")
            return
        
        # 不发送开始报告，只在最终报告中显示结果
        all_btn_texts = [bt[2] for bt in button_positions]
        print(f"[点击任务] 开始执行点击，匹配账号数：{len(matched_accounts)}，按钮文本：{', '.join(all_btn_texts[:3])}")
        
        # 并发控制：同时最多8个账号点击（在防封前提下最大化性能）
        # 通过延迟和抖动来分散请求，避免同时触发
        click_semaphore = asyncio.Semaphore(8)
        success_count = 0
        fail_count = 0
        success_accounts = []  # 记录成功的账号
        fail_accounts = []  # 记录失败的账号
        
        async def click_with_account(acc, btn_row, btn_col, btn_text, index):
            nonlocal success_count, fail_count, success_accounts, fail_accounts
            acc_id = acc['id']
            acc_name = acc.get('username') or acc.get('phone') or f"#{acc_id}"
            
            print(f"[点击任务] 🎯 账号 {acc_name} (#{acc_id}) 开始点击任务 (索引: {index}, 按钮: [{btn_row},{btn_col}] '{btn_text}')")
            
            async with click_semaphore:
                print(f"[点击任务] 账号 {acc_name} 获取信号量，开始执行")
                
                # 获取账号客户端
                client = manager.account_clients.get(acc_id)
                if not client:
                    print(f"[点击任务] ❌ 账号 {acc_name} 客户端不存在")
                    fail_count += 1
                    fail_accounts.append(f"{acc_name}: 客户端不存在")
                    return
                
                # 检查客户端是否真正连接
                try:
                    if not client.is_connected():
                        print(f"[点击任务] ❌ 账号 {acc_name} 客户端未连接")
                        fail_count += 1
                        fail_accounts.append(f"{acc_name}: 客户端未连接")
                        return
                except Exception as conn_check_error:
                    print(f"[点击任务] ⚠️ 账号 {acc_name} 检查连接状态失败: {conn_check_error}")
                    fail_count += 1
                    fail_accounts.append(f"{acc_name}: 连接状态异常")
                    return
                
                print(f"[点击任务] ✅ 账号 {acc_name} 客户端已连接")
                
                try:
                    # 全速运行：移除所有延迟，立即点击
                    # 不再等待，直接执行点击
                    
                    # 获取消息
                    print(f"[点击任务] 账号 {acc_name} 开始获取消息: chat_id={target_chat_id}, msg_id={target_msg_id}")
                    try:
                        acc_msg = await client.get_messages(target_chat_id, ids=target_msg_id)
                        if not acc_msg:
                            raise Exception('消息不存在或账号无法访问该消息')
                        print(f"[点击任务] ✅ 账号 {acc_name} 成功获取消息")
                    except Exception as e:
                        print(f"[点击任务] ❌ 账号 {acc_name} 获取消息失败: {e}")
                        fail_count += 1
                        error_str = str(e)
                        # 判断具体错误类型
                        if 'CHANNEL_PRIVATE' in error_str or 'CHAT_FORBIDDEN' in error_str or 'USER_BANNED_IN_CHANNEL' in error_str:
                            error_msg = '未加入群组/频道或已被禁止'
                        elif 'MESSAGE_NOT_FOUND' in error_str or 'MSG_ID_INVALID' in error_str:
                            error_msg = '消息不存在或无效'
                        else:
                            error_msg = error_str[:50]
                        fail_accounts.append(f"{acc_name}: {error_msg}")
                        # 不发送单个账号的失败消息，只在最终报告中显示
                        return
                    
                    # 点击按钮
                    print(f"[点击任务] 🖱️ 账号 {acc_name} 准备点击按钮 [{btn_row},{btn_col}] '{btn_text}'")
                    try:
                        await acc_msg.click(btn_row, btn_col)
                        success_count += 1
                        success_accounts.append(acc_name)
                        print(f"[点击任务] ✅ 账号 {acc_name} 点击成功！")
                        # 不发送单个账号的成功消息，只在最终报告中显示
                    except Exception as e:
                        print(f"[点击任务] ❌ 账号 {acc_name} 点击失败: {type(e).__name__}: {e}")
                        fail_count += 1
                        fail_accounts.append(f"{acc_name}: {str(e)[:50]}")
                        # 不发送单个账号的失败消息，只在最终报告中显示
                except Exception as e:
                    print(f"[点击任务] ❌ 账号 {acc_name} 处理过程出错: {type(e).__name__}: {e}")
                    import traceback
                    traceback.print_exc()
                    fail_count += 1
                    fail_accounts.append(f"{acc_name}: {str(e)[:50]}")
                    # 不发送单个账号的失败消息，只在最终报告中显示
        
        # 优化：将点击账号分成多个批次，每批次并发执行
        # 每个批次约10个账号，充分利用CPU和内存
        accounts_per_batch = 10
        total_accounts = len(matched_accounts)
        num_batches = max(1, (total_accounts + accounts_per_batch - 1) // accounts_per_batch)
        
        print(f"[点击任务] 🎯 开始执行点击，共 {total_accounts} 个账号需要点击，分成 {num_batches} 个批次（每批次约 {accounts_per_batch} 个账号）")
        
        # 将账号列表分成多个批次
        account_batches = []
        for i in range(0, total_accounts, accounts_per_batch):
            batch = matched_accounts[i:i + accounts_per_batch]
            account_batches.append(batch)
        
        # 定义批次点击函数
        async def click_batch(batch_accounts, batch_index):
            """执行一个批次的点击任务（并发）"""
            try:
                batch_tasks = [click_with_account(acc, btn_row, btn_col, btn_text, idx) 
                              for idx, (acc, btn_row, btn_col, btn_text) in enumerate(batch_accounts, start=batch_index * accounts_per_batch)]
                batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                
                batch_success = sum(1 for r in batch_results if not isinstance(r, Exception))
                batch_fail = len(batch_results) - batch_success
                print(f"[点击批次 #{batch_index + 1}] 完成: 成功 {batch_success} 个，失败 {batch_fail} 个")
                return batch_results
            except Exception as e:
                print(f"[点击批次 #{batch_index + 1}] 执行出错: {e}")
                return []
        
        # 所有批次并发执行（充分利用CPU和内存）
        batch_tasks = [click_batch(batch, idx) for idx, batch in enumerate(account_batches)]
        all_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
        
        # 展平所有批次的结果
        results = []
        for batch_results in all_results:
            if isinstance(batch_results, list):
                results.extend(batch_results)
            elif isinstance(batch_results, Exception):
                print(f"[点击任务] ⚠️ 批次执行异常: {batch_results}")
        
        print(f"[点击任务] 所有点击任务执行完成，共处理 {len(results)} 个结果")
        
        # 检查是否有异常
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                print(f"[点击任务] ⚠️ 任务 {i} 执行异常: {result}")
        
        # 发送完成报告
        try:
            # 构建详细的完成报告
            total_accounts = len(accounts)
            matched_count = len(matched_accounts)
            all_btn_texts = [bt[2] for bt in button_positions]
            
            report_msg = (
                f'✅ **点击任务完成**\n'
                f'━━━━━━━━━━━━━━━━\n'
                f'📋 **消息信息**\n'
                f'• Chat ID: `{target_chat_id}`\n'
                f'• Message ID: `{target_msg_id}`\n'
                f'• 按钮文本: {", ".join(all_btn_texts[:3])}{"..." if len(all_btn_texts) > 3 else ""}\n\n'
                f'📊 **执行统计**\n'
                f'• 总账号数: {total_accounts} 个\n'
                f'• 匹配账号数: {matched_count} 个\n'
                f'• ✅ 成功: {success_count} 个\n'
                f'• ❌ 失败: {fail_count} 个\n'
            )
            
            # 显示成功的账号
            if success_accounts:
                report_msg += f'\n✅ **成功账号** ({len(success_accounts)} 个):\n'
                for acc in success_accounts:
                    report_msg += f'• {acc}\n'
            
            # 显示失败的账号
            if fail_accounts:
                report_msg += f'\n❌ **失败账号** ({len(fail_accounts)} 个):\n'
                for acc_info in fail_accounts[:10]:  # 最多显示10个
                    report_msg += f'• {acc_info}\n'
                if len(fail_accounts) > 10:
                    report_msg += f'• ... 还有 {len(fail_accounts) - 10} 个失败\n'
            
            await bot.send_message(report_chat_id, report_msg, parse_mode='markdown')
        except Exception as send_error:
            print(f"[点击任务] ⚠️ 发送完成报告失败: {send_error}")
    except Exception as e:
        print(f"[点击任务] ❌ 任务出错: {e}")
        import traceback
        traceback.print_exc()
        try:
            error_detail = (
                f'❌ **点击任务执行出错**\n'
                f'━━━━━━━━━━━━━━━━\n'
                f'错误类型：`{type(e).__name__}`\n'
                f'错误信息：`{str(e)}`\n\n'
                f'请检查日志获取更多信息。'
            )
            await bot.send_message(report_chat_id, error_detail, parse_mode='markdown')
        except Exception as send_error:
            print(f"[点击任务] ❌ 发送错误消息也失败: {send_error}")


async def setup_handlers(manager: ClientManager):
    """设置机器人事件处理器（防止重复注册）"""
    # 如果已经设置过，直接返回
    if manager._handlers_setup:
        print("[警告] 事件处理器已经设置过，跳过重复注册")
        return
    
    bot = manager.bot
    if not bot:
        raise RuntimeError("Bot 未初始化，请先调用 start_control_bot()")
    
    # 标记为已设置
    manager._handlers_setup = True

    def keywords_label(kind: str) -> str:
        return '监听' if kind == 'listen' else '点击'

    def keywords_overview_text(account_id: int, kind: str) -> str:
        items = settings_service.get_account_keywords(account_id, kind=kind) or []
        preview = '\n'.join(['• ' + k for k in items[:30]]) or '（空）'
        return (
            f"📌 当前关键字（{keywords_label(kind)}）共 {len(items)} 条（预览前30条）：\n{preview}\n\n"
            "操作说明：\n"
            "• 发送关键字列表（换行/逗号分隔）=> 全量覆盖\n"
            "• 发送 “+词1,词2” => 追加关键字\n"
            "• 发送 “-词1” 或 “q词1” => 删除关键字\n"
            "• 发送 “导入” => 上传文本文件批量追加\n"
            "• 发送 “导出” => 查看全部关键字\n"
            "• 发送 “完成” => 返回主菜单"
        )

    async def open_keywords_editor(chat_id: int, account_id: int, kind: str, *, via_callback=None):
        set_state(chat_id, 'keywords_manage', account_id=account_id, kind=kind)
        text = keywords_overview_text(account_id, kind)
        if via_callback:
            try:
                await via_callback.edit(text)
            except Exception:
                await bot.send_message(chat_id, text)
            else:
                await via_callback.answer('✅ 已选择账号')
                return
        await bot.send_message(chat_id, text)

    async def start_bulk_keywords(event, kind: str):
        role_filter = 'listen' if kind == 'listen' else 'click'
        rows = list_accounts(role_filter)
        if not rows:
            await event.respond(f'⚠️ 尚无{keywords_label(kind)}账号，请先添加。')
            return
        
        # 显示当前所有账号的关键词
        if kind == 'click':
            # 点击关键词：显示全局关键词
            global_keywords = settings_service.get_global_click_keywords()
            total_keywords = len(global_keywords)
            if global_keywords:
                preview = ', '.join(global_keywords[:10])
                if len(global_keywords) > 10:
                    preview += f' ... (共 {len(global_keywords)} 个)'
                current_status = f'**全局点击关键词（应用到所有 {len(rows)} 个点击账号）：**\n• {preview}'
            else:
                current_status = f'**全局点击关键词：**（未设置）\n\n当前有 {len(rows)} 个点击账号'
        else:
            # 监听关键词：显示每个账号的关键词
            lines = []
            total_keywords = 0
            for r in rows:
                acc_id = r['id']
                ident = r['username'] or r['phone'] or f"#{acc_id}"
                keywords = settings_service.get_account_keywords(acc_id, kind=kind) or []
                total_keywords += len(keywords)
                if keywords:
                    preview = ', '.join(keywords[:5])
                    if len(keywords) > 5:
                        preview += f' ... (共 {len(keywords)} 个)'
                    lines.append(f"• {ident}: {preview}")
                else:
                    lines.append(f"• {ident}: （无）")
            
            current_status = '\n'.join(lines) if lines else '（所有账号都未设置关键词）'
        
        set_state(event.chat_id, 'bulk_keywords_input', kind=kind)
        if kind == 'click':
            await event.respond(
                f"📢 批量添加 {keywords_label(kind)} 关键字（全局设置）\n\n"
                f"{current_status}\n\n"
                "操作说明：\n"
                "• 发送关键字列表（换行/逗号分隔）将追加到全局关键词并应用到所有点击账号\n"
                "• 发送 “-关键词1,关键词2” 将从全局关键词中删除\n"
                "• 发送 “清空” 将清空全局关键词\n"
                "• 发送 “完成” 返回主菜单"
            )
        else:
            await event.respond(
                f"📢 批量添加 {keywords_label(kind)} 关键字\n\n"
                f"当前关键词（共 {len(rows)} 个账号，{total_keywords} 个关键词）：\n{current_status}\n\n"
                "操作说明：\n"
                "• 发送关键字列表（换行/逗号分隔）将追加到所有对应账号\n"
                "• 发送 “-关键词1,关键词2” 将删除指定关键词\n"
                "• 发送 “清空” 将删除所有对应账号的该类关键字\n"
                "• 发送 “完成” 返回主菜单（会显示关键词统计）"
            )

    async def refresh_account_card(event, acc_id: int, *, to_menu: bool):
        row = dao_accounts.get(acc_id)
        if not row:
            await event.answer('账号不存在', alert=True)
            try:
                await event.edit('⚠️ 该账号已不存在', buttons=None)
            except Exception:
                pass
            return None
        text = account_menu_text(row) if to_menu else account_summary_text(row)
        buttons = account_menu_buttons(acc_id) if to_menu else account_base_buttons(acc_id)
        try:
            await event.edit(text, buttons=buttons)
        except Exception:
            await bot.send_message(event.chat_id, text, buttons=buttons)
        return row

    async def remove_account(acc_id: int):
        client = manager.account_clients.pop(acc_id, None)
        if client:
            try:
                await asyncio.wait_for(client.disconnect(), timeout=2.0)
            except Exception:
                pass
        dao_keywords.set_keywords(acc_id, [], kind='listen')
        dao_keywords.set_keywords(acc_id, [], kind='click')
        settings_service.clear_listen_sources(acc_id)
        settings_service.clear_account_settings(acc_id)
        dao_accounts.delete(acc_id)

    @bot.on(events.NewMessage(pattern='/start'))
    async def _(event):
        await event.respond(
            '🙌 欢迎使用控制面板\n\n'
            '功能一览：\n'
            '• 🧩 监听/点击关键词管理\n'
            '• ➕ 添加账号（支持 StringSession 文本 或 .session 文件）\n'
            '• 🎯 设置目标机器人（所有账号批量 /start）\n'
            '• 📝 模板消息、🐢 发送延迟、⚙️ 并发数、▶️ 开始发送\n'
            '• 🚪 自动进群、🗑️ 移除账号\n\n'
            '👇 请选择功能：',
            buttons=main_keyboard()
        )

    @bot.on(events.CallbackQuery(pattern=b'start_all:(on|off)'))
    async def _(event):
        action = event.pattern_match.group(1).decode()
        rows = list_accounts('click')
        if not rows:
            await event.answer('暂无点击账号', alert=True)
            try:
                await event.edit('⚠️ 暂无点击账号，请先添加。', buttons=None)
            except Exception:
                pass
            return
        enable = action == 'on'
        for r in rows:
            settings_service.set_start_sending(enable, r['id'])
        
        # If enabling, send /start messages to target bot
        if enable:
            bot_username = settings_service.get_target_bot()
            if not bot_username:
                msg = '⚠️ 请先设置目标机器人（点击"🎯 设置目标机器人"）'
                try:
                    await event.edit(msg, buttons=None)
                except Exception:
                    await bot.send_message(event.chat_id, msg)
                await event.answer('需要先设置目标机器人', alert=True)
                return
            
            target = f"@{bot_username}"
            click_accounts = [acc_id for acc_id, client in list(manager.account_clients.items()) if role_allows_click(get_account_role(acc_id))]
            if not click_accounts:
                msg = '⚠️ 当前没有激活的点击账号，无法发送消息'
                try:
                    await event.edit(msg, buttons=None)
                except Exception:
                    await bot.send_message(event.chat_id, msg)
                await event.answer('无激活账号', alert=True)
                return
            
            await event.answer('⏳ 正在发送，请稍候…')
            ok = 0
            fail_details = []
            for acc_id in click_accounts:
                client = manager.account_clients.get(acc_id)
                if not client:
                    fail_details.append(f"账号 #{acc_id}: 客户端未连接")
                    continue
                try:
                    await client.send_message(target, '/start')
                    ok += 1
                except Exception as e:
                    acc_info = dao_accounts.get(acc_id)
                    acc_label = acc_info.get('username') or acc_info.get('phone') or f"#{acc_id}"
                    fail_details.append(f"账号 {acc_label}: {str(e)}")
            
            msg_parts = [
                f"{'✅ 已开启' if enable else '⏸️ 已关闭'} 所有点击账号的发送开关（共 {len(rows)} 个账号）"
            ]
            if enable:
                msg_parts.append(f"\n发送消息：/start\n目标用户：{target}")
                msg_parts.append(f"\n✅ 成功账号：{ok} 个")
                if fail_details:
                    msg_parts.append(f"\n❌ 失败账号：{len(fail_details)} 个")
                    msg_parts.append("\n失败详情：")
                    for detail in fail_details[:10]:  # Limit to first 10 errors
                        msg_parts.append(f"• {detail}")
                    if len(fail_details) > 10:
                        msg_parts.append(f"• ... 还有 {len(fail_details) - 10} 个失败")
            
            msg = '\n'.join(msg_parts)
            try:
                await event.edit(msg, buttons=None)
            except Exception:
                await bot.send_message(event.chat_id, msg)
        else:
            msg = f"{'✅ 已开启' if enable else '⏸️ 已关闭'} 所有点击账号的发送开关（共 {len(rows)} 个账号）。"
            try:
                await event.edit(msg, buttons=None)
            except Exception:
                await bot.send_message(event.chat_id, msg)
        await event.answer('完成')

    @bot.on(events.CallbackQuery(pattern=b'auto_join:(listen|click)'))
    async def _(event):
        role_sel = event.pattern_match.group(1).decode()
        rows = list_accounts('listen' if role_sel == 'listen' else 'click')
        active_ids = [r['id'] for r in rows if r['id'] in manager.account_clients]
        if not active_ids:
            await event.answer('暂无对应激活账号', alert=True)
            return
        set_state(event.chat_id, 'auto_join_wait_link', account_ids=active_ids, role=role_sel)
        text = (
            f"🚪 使用{'监听' if role_sel=='listen' else '点击'}账号自动进群\n"
            "请发送群链接或 @用户名（每行一个，可多个）\n支持：https://t.me/+inviteHash / https://t.me/groupname / @groupname"
        )
        try:
            await event.edit(text, buttons=None)
        except Exception:
            await bot.send_message(event.chat_id, text)
        await event.answer('请发送链接')

    @bot.on(events.CallbackQuery(pattern=b'remove_all_role:(listen|click|all|cancel)'))
    async def _(event):
        print(f"[移除账号] 收到回调: {event.data}")
        try:
            action = event.pattern_match.group(1).decode()
            print(f"[移除账号] 操作类型: {action}")
            
            if action == 'cancel':
                await event.answer('已取消')
                try:
                    await event.edit('✅ 已取消移除操作', buttons=None)
                except Exception:
                    pass
                return
            
            if action == 'listen':
                targets = list_accounts('listen')
                label = '监听'
            elif action == 'click':
                targets = list_accounts('click')
                label = '点击'
            else:
                targets = dao_accounts.list_all()
                label = '全部'
            
            print(f"[移除账号] 找到 {len(targets)} 个目标账号")
            
            if not targets:
                await event.answer('暂无可移除账号', alert=True)
                try:
                    await event.edit('⚠️ 暂无可移除账号', buttons=None)
                except Exception:
                    pass
                return
            
            await event.answer('⏳ 正在移除…')
            count = 0
            for r in targets:
                print(f"[移除账号] 正在移除账号 #{r['id']}")
                await remove_account(r['id'])
                count += 1
            
            msg = f"🗑️ 已移除 {label} 账号 {count} 个。"
            print(f"[移除账号] 移除完成: {msg}")
            try:
                await event.edit(msg, buttons=None)
            except Exception:
                await bot.send_message(event.chat_id, msg)
        except Exception as e:
            print(f"[移除账号] ❌ 处理回调时出错: {e}")
            import traceback
            traceback.print_exc()
            try:
                await event.answer(f'❌ 移除失败：{e}', alert=True)
            except:
                pass

    @bot.on(events.CallbackQuery(pattern=b'acc\\|'))
    async def _(event):
        data = event.data.decode()
        parts = data.split('|')
        if len(parts) < 3:
            await event.answer()
            return
        _, acc_id_str, action = parts[0], parts[1], parts[2]
        try:
            acc_id = int(acc_id_str)
        except ValueError:
            await event.answer('参数无效', alert=True)
            return
        role = get_account_role(acc_id)
        row = dao_accounts.get(acc_id)
        if not row and action != 'delete_confirm':
            await event.answer('账号不存在', alert=True)
            try:
                await event.edit('⚠️ 账号不存在', buttons=None)
            except Exception:
                pass
            return

        if action == 'menu':
            await refresh_account_card(event, acc_id, to_menu=True)
            await event.answer()
            return
        if action == 'back':
            await refresh_account_card(event, acc_id, to_menu=False)
            await event.answer()
            return
        if action == 'kwl':
            if not role_allows_listen(role):
                await event.answer('该账号不是监听账号', alert=True)
                return
            await open_keywords_editor(event.chat_id, acc_id, 'listen', via_callback=event)
            return
        if action == 'kwc':
            if not role_allows_click(role):
                await event.answer('该账号不是点击账号', alert=True)
                return
            await open_keywords_editor(event.chat_id, acc_id, 'click', via_callback=event)
            return
        if action == 'tmpl':
            if not role_allows_click(role):
                await event.answer('仅点击账号支持设置发送消息', alert=True)
                return
            set_state(event.chat_id, 'set_template_input', account_id=acc_id)
            await bot.send_message(event.chat_id, '📝 请输入发送消息模板（文本）')
            await event.answer('请输入新模板')
            return
        if action == 'delay':
            if not role_allows_click(role):
                await event.answer('仅点击账号支持设置发送延迟', alert=True)
                return
            set_state(event.chat_id, 'set_send_delay_input', account_id=acc_id)
            await bot.send_message(event.chat_id, '🐢 请输入发送延迟（单位秒，可为小数）')
            await event.answer('请输入发送延迟')
            return
        if action == 'clickdelay':
            if not role_allows_click(role):
                await event.answer('仅点击账号支持设置点击延迟', alert=True)
                return
            set_state(event.chat_id, 'set_click_delay_input', account_id=acc_id)
            await bot.send_message(event.chat_id, '⏱️ 请输入点击延迟（单位秒，可为小数，例如 0.8）')
            await event.answer('请输入点击延迟')
            return
        if action == 'start':
            if not role_allows_click(role):
                await event.answer('仅点击账号支持发送开关', alert=True)
                return
            current = settings_service.get_start_sending(acc_id)
            settings_service.set_start_sending(not current, acc_id)
            await event.answer('✅ 已开启' if not current else '⏸️ 已关闭')
            await refresh_account_card(event, acc_id, to_menu=True)
            return
        if action == 'delete':
            buttons = [
                [Button.inline('❌ 确认删除', data=f'acc|{acc_id}|delete_confirm')],
                [Button.inline('⬅️ 返回', data=f'acc|{acc_id}|menu')]
            ]
            try:
                await event.edit(f'⚠️ 确认删除账号 #{acc_id}？该操作不可恢复。', buttons=buttons)
            except Exception:
                await bot.send_message(event.chat_id, f'⚠️ 确认删除账号 #{acc_id}？', buttons=buttons)
            await event.answer()
            return
        if action == 'delete_confirm':
            await remove_account(acc_id)
            try:
                await event.edit(f'✅ 账号 #{acc_id} 已删除', buttons=None)
            except Exception:
                await bot.send_message(event.chat_id, f'✅ 账号 #{acc_id} 已删除')
            await event.answer('已删除')
            return

    @bot.on(events.NewMessage(incoming=True))
    async def _(event):
        chat_id = event.chat_id
        text = (event.raw_text or '').strip()
        
        # 排除 /start 命令，避免重复处理（已在上面单独处理）
        if text == '/start' or text.startswith('/start '):
            return
        
        st = get_state(chat_id)

        # 如果没有状态，检查是否是链接，如果是就自动执行点击
        if not st:
            # 检查是否是消息链接格式
            link_patterns = [
                r't\.me/c/(\d+)/(\d+)',  # t.me/c/xxx/123
                r't\.me/([a-zA-Z0-9_]+)/(\d+)',  # t.me/username/123
                r'https?://t\.me/c/(\d+)/(\d+)',  # https://t.me/c/xxx/123
                r'https?://t\.me/([a-zA-Z0-9_]+)/(\d+)',  # https://t.me/username/123
            ]
            
            is_link = False
            for pattern in link_patterns:
                if re.search(pattern, text):
                    is_link = True
                    break
            
            if is_link:
                print(f"[自动点击] 检测到链接，自动执行点击: {text}")
                success, error_msg = await parse_and_execute_click(manager, text, chat_id)
                if success:
                    await event.respond('🚀 **已自动识别链接，开始点击任务**', parse_mode='markdown', buttons=main_keyboard())
                else:
                    await event.respond(f'⚠️ **自动点击失败**\n\n{error_msg}', parse_mode='markdown', buttons=main_keyboard())
                return

        # 如果在 set_target_bot 模式下且输入包含 emoji，直接拒绝（可能是按钮点击）
        if st and st.get('mode') == 'set_target_bot':
            if any(unicodedata.category(c) == 'So' for c in text):
                await event.respond('⚠️ 请直接输入用户名，不要点击按钮', buttons=None)
                return
        
        # 主菜单按钮文本
        MAIN_MENU_COMMANDS = {
            '🧩 监听关键词', '🧩 点击关键词',
            '📒 账号列表',
            '➕ 添加监听账号', '➕ 添加点击账号',
            '📤 设置转发目标',
            '📝 设置发送消息', '🐢 设置发送延迟',
            '⏱️ 设置点击延迟',
            '▶️ 开始发送',
            '🎯 设置目标机器人', '🚪 自动进群',
            '🗑️ 移除所有账号'
        }
        
        # 检查是否为主菜单命令
        is_main_menu_cmd = False
        for cmd in MAIN_MENU_COMMANDS:
            if is_cmd(text, cmd):
                is_main_menu_cmd = True
                break
        
        # 进行中的状态优先处理
        if st:
            mode = st['mode']
            
            # 如果用户在添加账号状态下发送主菜单命令，清除状态并允许命令执行
            if is_main_menu_cmd and mode in ('add_listen_account_wait_string', 'add_click_account_wait_file'):
                set_state(chat_id, None)
                st = None
                # 继续执行，让命令处理器处理（不在这里 return）
            else:
                # 正常处理状态
                if mode == 'bulk_keywords_input':
                    kind = st['pending']['kind']
                    t = (text or '').strip()
                    rows = list_accounts('listen' if kind == 'listen' else 'click')
                    if not rows:
                        set_state(chat_id)
                        await event.respond('⚠️ 当前没有可用账号，请先添加。', buttons=main_keyboard())
                        return
                    if t in ('完成', '返回'):
                        # 显示当前关键词统计
                        if kind == 'click':
                            # 点击关键词：显示全局关键词
                            global_keywords = settings_service.get_global_click_keywords()
                            total_keywords = len(global_keywords)
                            if global_keywords:
                                preview = ', '.join(global_keywords[:20])
                                if len(global_keywords) > 20:
                                    preview += f' ... (共 {len(global_keywords)} 个)'
                                summary_text = f'全局点击关键词：{preview}'
                            else:
                                summary_text = '全局点击关键词：（未设置）'
                            
                            set_state(chat_id)
                            await event.respond(
                                f'✅ **已返回主菜单**\n\n'
                                f'📊 **当前{keywords_label(kind)}关键词统计：**\n'
                                f'点击账号数：{len(rows)} 个\n'
                                f'全局关键词总数：{total_keywords} 个\n\n'
                                f'**{summary_text}**\n\n'
                                f'💡 提示：全局点击关键词会自动应用到所有点击账号',
                                buttons=main_keyboard(),
                                parse_mode='markdown'
                            )
                        else:
                            # 监听关键词：显示每个账号的关键词
                            total_keywords = 0
                            account_keywords_summary = []
                            for r in rows:
                                acc_id = r['id']
                                keywords = settings_service.get_account_keywords(acc_id, kind=kind) or []
                                total_keywords += len(keywords)
                                ident = r.get('username') or r.get('phone') or f"#{acc_id}"
                                if keywords:
                                    preview = ', '.join(keywords[:5])
                                    if len(keywords) > 5:
                                        preview += f' ... (共 {len(keywords)} 个)'
                                    account_keywords_summary.append(f"• {ident}: {preview}")
                                else:
                                    account_keywords_summary.append(f"• {ident}: （无）")
                            
                            summary_text = '\n'.join(account_keywords_summary) if account_keywords_summary else '（所有账号都未设置关键词）'
                            set_state(chat_id)
                            await event.respond(
                                f'✅ **已返回主菜单**\n\n'
                                f'📊 **当前{keywords_label(kind)}关键词统计：**\n'
                                f'账号数：{len(rows)} 个\n'
                                f'关键词总数：{total_keywords} 个\n\n'
                                f'**各账号关键词：**\n{summary_text}',
                                buttons=main_keyboard(),
                                parse_mode='markdown'
                            )
                        return
                    if t.lower() in ('清空', 'clear'):
                        if kind == 'click':
                            # 清空全局点击关键词
                            settings_service.set_global_click_keywords([])
                            # 应用到所有点击账号
                            settings_service.apply_global_click_keywords_to_all_accounts()
                            set_state(chat_id)
                            await event.respond(f"🧹 已清空所有点击账号的关键字（全局设置）", buttons=main_keyboard())
                        else:
                            # 监听关键词保持原有逻辑
                            for r in rows:
                                dao_keywords.set_keywords(r['id'], [], kind=kind)
                            set_state(chat_id)
                            await event.respond(f"🧹 已清空 {len(rows)} 个{keywords_label(kind)}账号的关键字", buttons=main_keyboard())
                        return
                    # 支持单独删除关键词：-关键词 或 -关键词1,关键词2
                    if t.startswith('-') or t.startswith('－'):
                        # 删除关键词
                        parts = split_keywords_payload(t[1:].strip())
                        if not parts:
                            await event.respond('⚠️ 请提供要删除的关键字，格式：-关键词1,关键词2')
                            return
                        
                        if kind == 'click':
                            # 从全局关键词中删除
                            for word in parts:
                                settings_service.delete_global_click_keyword(word)
                            # 应用到所有点击账号
                            settings_service.apply_global_click_keywords_to_all_accounts()
                            global_keywords = settings_service.get_global_click_keywords()
                            set_state(chat_id)
                            await event.respond(
                                f"🗑️ 已从全局点击关键词中删除 {len(parts)} 条关键字\n"
                                f"当前全局点击关键词：{', '.join(global_keywords[:10])}{'...' if len(global_keywords) > 10 else ''}",
                                buttons=main_keyboard()
                            )
                        else:
                            # 监听关键词保持原有逻辑
                            deleted_count = 0
                            for r in rows:
                                for word in parts:
                                    before = settings_service.get_account_keywords(r['id'], kind=kind) or []
                                    settings_service.delete_keyword(r['id'], word, kind=kind)
                                    after = settings_service.get_account_keywords(r['id'], kind=kind) or []
                                    if len(before) > len(after):
                                        deleted_count += 1
                            set_state(chat_id)
                            await event.respond(
                                f"🗑️ 已从 {len(rows)} 个{keywords_label(kind)}账号中删除 {deleted_count} 条关键字",
                                buttons=main_keyboard()
                            )
                        return
                    parts = split_keywords_payload(t)
                    if not parts:
                        await event.respond('⚠️ 请发送关键字内容，或发送"完成"返回主菜单。\n💡 提示：使用 "-关键词" 可以单独删除关键词')
                        return
                    
                    # 对于点击关键词，设置为全局关键词并应用到所有点击账号
                    if kind == 'click':
                        # 追加到全局关键词
                        for word in parts:
                            settings_service.add_global_click_keyword(word)
                        # 应用到所有点击账号
                        settings_service.apply_global_click_keywords_to_all_accounts()
                        global_keywords = settings_service.get_global_click_keywords()
                        set_state(chat_id)
                        await event.respond(
                            f"✅ 已为所有点击账号追加 {len(parts)} 条关键字（全局设置）\n"
                            f"当前全局点击关键词：{', '.join(global_keywords[:10])}{'...' if len(global_keywords) > 10 else ''}",
                            buttons=main_keyboard()
                        )
                    else:
                        # 监听关键词保持原有逻辑（每个账号单独设置）
                        for r in rows:
                            for word in parts:
                                settings_service.add_keyword(r['id'], word, kind=kind)
                        set_state(chat_id)
                        await event.respond(
                            f"✅ 已为 {len(rows)} 个{keywords_label(kind)}账号追加 {len(parts)} 条关键字",
                            buttons=main_keyboard()
                        )
                    return

                elif mode == 'choose_account_role':
                    account_id = st['pending']['account_id']
                    t = text.strip()
                    if t in ('监听账号', '监听', 'listen'):
                        settings_service.set_account_role(account_id, 'listen')
                        set_state(chat_id, 'set_account_target', account_id=account_id)
                        await event.respond('🎯 该账号为"监听账号"。请输入此账号的提醒目标（chat_id 或 @username）。\n提示：留空或发送"全局"将使用全局目标。')
                        return
                    if t in ('点击账号', '点击', 'click'):
                        settings_service.set_account_role(account_id, 'click')
                        # 自动应用全局点击关键词
                        settings_service.apply_global_click_keywords_to_account(account_id)
                        set_state(chat_id)
                        await event.respond('✅ 已设置为"点击账号"，已自动应用全局点击关键词', buttons=main_keyboard())
                        return
                    if t in ('同时监听与点击', 'both'):
                        settings_service.set_account_role(account_id, 'both')
                        # 自动应用全局点击关键词
                        settings_service.apply_global_click_keywords_to_account(account_id)
                        set_state(chat_id, 'set_account_target', account_id=account_id)
                        await event.respond('🎯 该账号为"同时"。请输入此账号的提醒目标（chat_id 或 @username）。\n提示：留空或发送"全局"将使用全局目标。\n✅ 已自动应用全局点击关键词')
                        return
                    if t in ('跳过', 'skip'):
                        set_state(chat_id)
                        await event.respond('已跳过角色设置（默认按全局策略处理）', buttons=main_keyboard())
                        return
                    await event.respond('请选择账号角色：', buttons=roles_keyboard())
                    return

                elif mode == 'change_account_role':
                    account_id = st['pending']['account_id']
                    t = text.strip()
                    if t in ('监听账号', '监听', 'listen'):
                        settings_service.set_account_role(account_id, 'listen')
                        set_state(chat_id)
                        await event.respond(f'✅ 账号 #{account_id} 已设置为"监听账号"', buttons=main_keyboard())
                        return
                    if t in ('点击账号', '点击', 'click'):
                        settings_service.set_account_role(account_id, 'click')
                        # 自动应用全局点击关键词
                        settings_service.apply_global_click_keywords_to_account(account_id)
                        set_state(chat_id)
                        await event.respond(f'✅ 账号 #{account_id} 已设置为"点击账号"，已自动应用全局点击关键词', buttons=main_keyboard())
                        return
                    if t in ('同时监听与点击', 'both'):
                        settings_service.set_account_role(account_id, 'both')
                        # 自动应用全局点击关键词
                        settings_service.apply_global_click_keywords_to_account(account_id)
                        set_state(chat_id)
                        await event.respond(f'✅ 账号 #{account_id} 已设置为"同时监听与点击"，已自动应用全局点击关键词', buttons=main_keyboard())
                        return
                    if t in ('取消', '退出', 'cancel'):
                        set_state(chat_id)
                        await event.respond('✅ 已取消', buttons=main_keyboard())
                        return
                    await event.respond('请选择账号角色：', buttons=roles_keyboard())
                    return

                elif mode == 'set_account_target':
                    account_id = st['pending']['account_id']
                    t = (text or '').strip()
                    if t in ('全局', 'global', ''):
                        settings_service.set_account_target_chat(account_id, None)
                        set_state(chat_id)
                        await event.respond('✅ 已设置为使用"全局提醒目标"', buttons=main_keyboard())
                        return
                    settings_service.set_account_target_chat(account_id, t)
                    set_state(chat_id)
                    await event.respond('✅ 已设置账号专属提醒目标', buttons=main_keyboard())
                    return

                elif mode == 'set_forward_target_global':
                    t = (text or '').strip()
                    if t in ('取消', '退出', 'cancel'):
                        set_state(chat_id)
                        await event.respond('✅ 已取消', buttons=main_keyboard())
                        return
                    if t in ('清空', 'clear'):
                        settings_service.set_target_chat('')
                        set_state(chat_id)
                        await event.respond('✅ 已清空转发目标', buttons=main_keyboard())
                        return
                    
                    # 检查是否是邀请链接
                    if t.startswith('https://t.me/+') or t.startswith('https://t.me/joinchat/') or t.startswith('t.me/+') or t.startswith('t.me/joinchat/'):
                        set_state(chat_id)
                        await event.respond(
                            '❌ **不能使用邀请链接**\n\n'
                            '机器人无法解析邀请链接。请使用：\n'
                            '• 公开群组/频道：@groupname\n'
                            '• 私有群组/频道：Chat ID（如 -1001234567890）\n\n'
                            '💡 获取 Chat ID：使用命令 `诊断群组 #账号ID`',
                            parse_mode='markdown',
                            buttons=main_keyboard()
                        )
                        return
                    
                    # 处理输入：支持 @username, chat_id, https://t.me/username
                    clean_target = t.strip()
                    
                    # 检查是否是 Chat ID（数字格式，包括负数）
                    is_chat_id = False
                    try:
                        chat_id_int = int(clean_target)
                        is_chat_id = True
                        print(f"[设置转发目标] 检测到 Chat ID 格式: {chat_id_int}")
                    except ValueError:
                        pass
                    
                    if not is_chat_id:
                        # 处理 URL 格式
                        if clean_target.startswith('http://') or clean_target.startswith('https://'):
                            # 提取用户名或处理邀请链接
                            if '/joinchat/' in clean_target or '/+' in clean_target:
                                set_state(chat_id)
                                await event.respond(
                                    '❌ **不能使用邀请链接**\n\n'
                                    '请使用群组/频道用户名（@groupname）或 Chat ID',
                                    parse_mode='markdown',
                                    buttons=main_keyboard()
                                )
                                return
                            else:
                                clean_target = clean_target.rsplit('/', 1)[-1]
                        
                        # 处理 @ 前缀
                        if clean_target.startswith('@'):
                            clean_target = clean_target[1:]
                    
                    # 设置全局转发目标（Chat ID 保持为字符串格式，Telethon 会自动处理）
                    final_target = str(chat_id_int) if is_chat_id else clean_target
                    settings_service.set_target_chat(final_target)
                    set_state(chat_id)
                    
                    if is_chat_id:
                        await event.respond(
                            f'✅ **转发目标已设置**\n\n'
                            f'Chat ID: `{final_target}`\n\n'
                            f'💡 请确保机器人已加入该群组/频道',
                            parse_mode='markdown',
                            buttons=main_keyboard()
                        )
                    else:
                        await event.respond(
                            f'✅ **转发目标已设置**\n\n'
                            f'目标: `@{final_target}`\n\n'
                            f'💡 请确保机器人已加入该群组/频道或有访问权限',
                            parse_mode='markdown',
                            buttons=main_keyboard()
                        )
                    return

                elif mode == 'set_target_chat':
                    settings_service.set_target_chat(text)
                    set_state(chat_id)
                    await event.respond('已设置提醒目标', buttons=main_keyboard())
                    return

                elif mode == 'set_target_bot':
                    t = (text or '').strip()
                    if not t:
                        await event.respond('⚠️ 请输入机器人用户名', buttons=None)
                        return
                    
                    # 允许取消
                    if t in ('取消', '退出', 'cancel', 'exit'):
                        set_state(chat_id)
                        await event.respond('✅ 已取消', buttons=main_keyboard())
                        return
                    
                    # 检查是否包含emoji（按钮文本）
                    has_emoji = any(unicodedata.category(c) == 'So' for c in t)
                    if has_emoji:
                        await event.respond('⚠️ 请直接输入用户名，不要点击按钮', buttons=None)
                        return
                    
                    # 处理输入
                    clean = t.lstrip('@')
                    if clean.startswith('http://') or clean.startswith('https://'):
                        clean = clean.rsplit('/', 1)[-1].lstrip('@')
                    
                    # 验证格式
                    if not re.match(r'^[a-zA-Z0-9_]{1,32}$', clean):
                        await event.respond('⚠️ 用户名格式无效，只能包含字母、数字、下划线', buttons=None)
                        return
                    
                    # 保存
                    try:
                        settings_service.set_target_bot(clean)
                        set_state(chat_id)
                        await event.respond(
                            f'✅ 目标机器人已设置：@{clean}\n\n'
                            '点击"▶️ 开始发送"按钮来批量发送消息。',
                            buttons=main_keyboard()
                        )
                    except Exception as e:
                        set_state(chat_id)
                        await event.respond(f'⚠️ 设置失败：{e}', buttons=main_keyboard())
                    return

                elif mode == 'set_global_template':
                    t = (text or '').strip()
                    if not t:
                        await event.respond('⚠️ 请输入消息内容', buttons=None)
                        return
                    if t in ('取消', '退出', 'cancel'):
                        set_state(chat_id)
                        await event.respond('✅ 已取消', buttons=main_keyboard())
                        return
                    settings_service.set_global_template(t)
                    set_state(chat_id)
                    await event.respond(
                        f'✅ 发送消息已设置：\n{t}\n\n'
                        '点击"▶️ 开始发送"按钮来批量发送消息。',
                        buttons=main_keyboard()
                    )
                    return

                elif mode == 'set_global_send_delay':
                    t = (text or '').strip()
                    if t in ('取消', '退出', 'cancel'):
                        set_state(chat_id)
                        await event.respond('✅ 已取消', buttons=main_keyboard())
                        return
                    try:
                        val = float(t)
                        if val < 0:
                            raise ValueError('延迟不能为负数')
                        settings_service.set_global_send_delay(val)
                        set_state(chat_id)
                        await event.respond(f'✅ 发送延迟已设置：{val} 秒', buttons=main_keyboard())
                    except ValueError:
                        await event.respond('⚠️ 请输入有效的数字（如 0.5、1、2）', buttons=None)
                    return

                elif mode == 'add_account_wait_file':
                    await event.respond('请发送 .session 文件作为文档（不是文本）')
                    return

                elif mode == 'add_listen_account_wait_string':
                    # 如果消息包含文件，让文件处理器处理，不在这里处理
                    if event.file:
                        return
                    tmsg = (text or '').strip()
                    if tmsg in ('完成', '结束', '返回'):
                        set_state(chat_id)
                        await event.respond('✅ 已结束添加', buttons=main_keyboard())
                        return
                    
                    # 检查是否为空或明显不是 StringSession
                    if not tmsg:
                        await event.respond('⚠️ 请输入 StringSession 文本，或发送 .session 文件（作为文档）\n发送"完成"可结束添加')
                        return
                    
                    # StringSession 通常以 "1A" 开头，如果不是，提示用户
                    if not tmsg.startswith('1'):
                        await event.respond(
                            '⚠️ 这看起来不是有效的 StringSession 文本。\n'
                            'StringSession 通常以 "1A" 开头。\n'
                            '请检查后重新发送，或发送 .session 文件（作为文档）。\n'
                            '发送"完成"可结束添加'
                        )
                        return
                    
                    try:
                        info = await manager.add_account_from_string_session(tmsg)
                        account_id = info['id']
                        # 如果账号已存在，合并角色；否则设置为 listen
                        current_role = settings_service.get_account_role(account_id) or 'both'
                        if info.get('existing'):
                            # 账号已存在，合并角色
                            if current_role == 'click':
                                settings_service.set_account_role(account_id, 'both')
                                role_msg = "（角色已合并为：监听+点击）"
                            elif current_role == 'listen':
                                role_msg = "（角色保持为：监听）"
                            else:
                                role_msg = f"（角色：{format_role_label(current_role)}）"
                        else:
                            # 新账号，设置为 listen
                            settings_service.set_account_role(account_id, 'listen')
                            role_msg = "（角色：监听）"
                        # 保持在连续添加模式
                        await event.respond(
                            f"✅ 监听账号添加成功！\n用户昵称：{info.get('nickname') or ''}\n用户名：{info.get('username') or '无'}\n账号：{info.get('phone') or ''}\n{role_msg}\n\n继续添加：发送 StringSession 文本或 .session 文件\n结束：发送「完成」\n（提醒目标可稍后在菜单中为该账号设置）"
                        )
                    except Exception as e:
                        # 解析失败，提示用户但保持状态，允许重试
                        error_msg = str(e)
                        await event.respond(
                            f"⚠️ 解析为 StringSession 失败：{error_msg}\n\n"
                            "请检查 StringSession 文本是否正确，或发送 .session 文件（作为文档）。\n"
                            '发送"完成"可结束添加'
                        )
                    return

                elif mode == 'add_click_account_wait_file':
                    # 如果消息包含文件，让文件处理器处理，不在这里处理
                    if event.file:
                        return
                    # 也支持文本 StringSession，作为点击账号
                    t = (text or '').strip()
                    if t in ('完成', '结束', '返回'):
                        set_state(chat_id)
                        await event.respond('✅ 已结束添加', buttons=main_keyboard())
                        return
                    
                    # 检查是否为空或明显不是 StringSession
                    if not t:
                        await event.respond('⚠️ 请输入 StringSession 文本，或发送 .session 文件（作为文档）\n发送"完成"可结束添加')
                        return
                    
                    # StringSession 通常以 "1A" 开头，如果不是，提示用户
                    if not t.startswith('1'):
                        await event.respond(
                            '⚠️ 这看起来不是有效的 StringSession 文本。\n'
                            'StringSession 通常以 "1A" 开头。\n'
                            '请检查后重新发送，或发送 .session 文件（作为文档）。\n'
                            '发送"完成"可结束添加'
                        )
                        return
                    
                    try:
                        info = await manager.add_account_from_string_session(t)
                        account_id = info['id']
                        # 如果账号已存在，合并角色；否则设置为 click
                        current_role = settings_service.get_account_role(account_id) or 'both'
                        if info.get('existing'):
                            # 账号已存在，合并角色
                            if current_role == 'listen':
                                settings_service.set_account_role(account_id, 'both')
                                role_msg = "（角色已合并为：监听+点击）"
                            elif current_role == 'click':
                                role_msg = "（角色保持为：点击）"
                            else:
                                role_msg = f"（角色：{format_role_label(current_role)}）"
                        else:
                            # 新账号，设置为 click
                            settings_service.set_account_role(account_id, 'click')
                            # 自动应用全局点击关键词
                            settings_service.apply_global_click_keywords_to_account(account_id)
                            role_msg = "（角色：点击，已应用全局点击关键词）"
                        # 保持在连续添加模式
                        await event.respond(
                            f"✅ 点击账号添加成功！\n用户昵称：{info.get('nickname') or ''}\n用户名：{info.get('username') or '无'}\n账号：{info.get('phone') or ''}\n{role_msg}\n\n继续添加：发送 StringSession 文本或 .session 文件\n结束：发送「完成」"
                        )
                    except Exception as e:
                        # 解析失败，提示用户但保持状态，允许重试
                        error_msg = str(e)
                        await event.respond(
                            f"⚠️ 解析为 StringSession 失败：{error_msg}\n\n"
                            "请检查 StringSession 文本是否正确，或发送 .session 文件（作为文档）。\n"
                            '发送"完成"可结束添加'
                        )
                    return

                elif mode == 'keywords_manage':
                    account_id = st['pending']['account_id']
                    kind = st['pending']['kind']
                    t = (text or '').strip()
                    if not t:
                        await event.respond('⚠️ 请发送指令，或发送"完成"返回主菜单。')
                        return
                    lower = t.lower()
                    if lower in ('完成', '返回'):
                        set_state(chat_id)
                        await event.respond('⬅️ 已返回主菜单', buttons=main_keyboard())
                        return
                    if lower in ('导出', 'export'):
                        cur = settings_service.get_account_keywords(account_id, kind=kind) or []
                        listing = '\n'.join(cur) or '（空）'
                        await event.respond(
                            f"当前关键字（{keywords_label(kind)}）共 {len(cur)} 条：\n{listing}"
                        )
                        return
                    if lower in ('导入', 'import'):
                        set_state(chat_id, 'keywords_import_wait_file', account_id=account_id, kind=kind)
                        await event.respond('📄 请发送包含关键字的文本文件（每行一个，支持逗号/换行分隔），作为文档上传。')
                        return
                    
                    before = set(settings_service.get_account_keywords(account_id, kind=kind) or [])
                    message = None
                    payload = t[1:] if t[:1] in ('+', '＋', '-', '－', 'q', 'Q') else t
                    if t.startswith(('+', '＋')):
                        parts = split_keywords_payload(payload)
                        if not parts:
                            await event.respond('⚠️ 请提供要追加的关键字')
                            return
                        for word in parts:
                            settings_service.add_keyword(account_id, word, kind=kind)
                        after = set(settings_service.get_account_keywords(account_id, kind=kind) or [])
                        message = f"✅ 已追加 {len(after - before)} 条关键字"
                    elif t.startswith(('-', '－')) or t.lower().startswith('q'):
                        parts = split_keywords_payload(payload)
                        if not parts:
                            await event.respond('⚠️ 请提供要删除的关键字')
                            return
                        for word in parts:
                            settings_service.delete_keyword(account_id, word, kind=kind)
                        after = set(settings_service.get_account_keywords(account_id, kind=kind) or [])
                        removed = max(0, len(before - after))
                        message = f"🗑️ 已删除 {removed} 条关键字"
                    else:
                        parts = split_keywords_payload(t)
                        dao_keywords.set_keywords(account_id, parts, kind=kind)
                        message = f"✅ 已覆盖关键字列表（共 {len(parts)} 条）"
                    await event.respond(message or '✅ 操作完成')
                    await event.respond(keywords_overview_text(account_id, kind))
                    return

                elif mode == 'set_click_delay_choose_account':
                    try:
                        acc_id = int(text)
                        row = dao_accounts.get(acc_id)
                        if not row:
                            await event.respond('账号不存在，请重新输入账号ID')
                            return
                        if not role_allows_click(get_account_role(acc_id)):
                            await event.respond('该账号不是点击账号，请重新输入账号ID')
                            return
                        set_state(chat_id, 'set_click_delay_input', account_id=acc_id)
                        await event.respond('⏱️ 请输入点击延迟（单位秒，可为小数，例如 0.8）')
                    except Exception:
                        await event.respond('⚠️ 请输入有效的账号ID（数字）')
                    return

                elif mode == 'set_click_delay_input':
                    account_id = st['pending']['account_id']
                    try:
                        value = float(text)
                        settings_service.set_click_delay(str(value), account_id)
                        set_state(chat_id)
                        await event.respond('✅ 已设置点击延迟', buttons=main_keyboard())
                    except Exception:
                        await event.respond('⚠️ 请输入数字，例如 0.8')
                    return

                elif mode == 'set_send_delay_choose_account':
                    try:
                        acc_id = int(text)
                        row = dao_accounts.get(acc_id)
                        if not row:
                            await event.respond('账号不存在，请重新输入账号ID')
                            return
                        if not role_allows_click(get_account_role(acc_id)):
                            await event.respond('该账号不是点击账号，请重新输入账号ID')
                            return
                        set_state(chat_id, 'set_send_delay_input', account_id=acc_id)
                        await event.respond('🐢 请输入发送延迟（单位秒，可为小数）')
                    except Exception:
                        await event.respond('⚠️ 请输入有效的账号ID（数字）')
                    return

                elif mode == 'set_send_delay_input':
                    account_id = st['pending']['account_id']
                    try:
                        value = float(text)
                        settings_service.set_send_delay(str(value), account_id)
                        set_state(chat_id)
                        await event.respond('✅ 已设置发送延迟', buttons=main_keyboard())
                    except Exception:
                        await event.respond('⚠️ 请输入数字，例如 1.2')
                    return

                elif mode == 'set_template_choose_account':
                    try:
                        acc_id = int(text)
                        row = dao_accounts.get(acc_id)
                        if not row:
                            await event.respond('账号不存在，请重新输入账号ID')
                            return
                        if not role_allows_click(get_account_role(acc_id)):
                            await event.respond('该账号不是点击账号，请重新输入账号ID')
                            return
                        set_state(chat_id, 'set_template_input', account_id=acc_id)
                        await event.respond('📝 请输入发送消息模板（文本）')
                    except Exception:
                        await event.respond('⚠️ 请输入有效的账号ID（数字）')
                    return

                elif mode == 'set_template_input':
                    account_id = st['pending']['account_id']
                    settings_service.set_template_message(text, account_id)
                    set_state(chat_id)
                    await event.respond('✅ 已设置发送消息模板', buttons=main_keyboard())
                    return

                elif mode == 'auto_join_wait_link':
                    link = text
                    account_ids = st['pending'].get('account_ids', [])
                    role_sel = st['pending'].get('role', 'listen')
                    if not account_ids:
                        set_state(chat_id)
                        await event.respond(
                            f"⚠️ 当前没有激活的{'监听' if role_sel == 'listen' else '点击'}账号，请先添加并连接成功。",
                            buttons=main_keyboard()
                        )
                        return
                    lines = [l.strip() for l in link.splitlines() if l.strip()]
                    if not lines:
                        await event.respond('⚠️ 请发送至少一个有效的群链接或用户名。')
                        return
                    ok = 0
                    fail = 0
                    mn, mx = settings_service.get_join_delay_range()
                    for target in lines:
                        for acc_id in account_ids:
                            client = manager.account_clients.get(acc_id)
                            if not client:
                                continue
                            try:
                                await joining.join_chat(client, target)
                                ok += 1
                            except Exception:
                                fail += 1
                            await asyncio.sleep(random.uniform(mn, mx))
                    set_state(chat_id)
                    msg = (
                        f"✅ 批量进群完成（使用{'监听' if role_sel=='listen' else '点击'}账号）\n"
                        '────────────\n'
                        f'处理链接：{len(lines)} 个\n'
                        f'✅ 成功次数：{ok}\n'
                        f'❌ 失败次数：{fail}'
                    )
                    await event.respond(msg, buttons=main_keyboard())
                    return

        if is_cmd(text, '设置转发目标'):
            # 显示当前全局转发目标
            cur_target = settings_service.get_target_chat() or '（未设置）'
            set_state(chat_id, 'set_forward_target_global')
            await event.respond(
                f'📤 设置转发目标\n\n'
                f'当前转发目标：{cur_target}\n\n'
                f'────────────\n'
                f'请输入转发目标（所有监听账号将发送到此目标）：\n\n'
                f'**支持的格式：**\n'
                f'• **公开群组/频道：** @groupname 或 groupname\n'
                f'• **私有群组/频道：** Chat ID（如 -1001234567890）\n'
                f'• **链接：** https://t.me/username\n\n'
                f'**如何获取 Chat ID：**\n'
                f'• 使用命令：`诊断群组 #账号ID`\n'
                f'• 或使用第三方机器人（如 @userinfobot）\n'
                f'• 确保机器人已加入目标群组/频道\n\n'
                f'**注意：**\n'
                f'• ❌ 不能使用邀请链接（t.me/+...）\n'
                f'• ✅ 私有群组必须使用 Chat ID\n'
                f'• ✅ 机器人必须在目标群组/频道中\n\n'
                f'• 输入"清空"清除设置\n'
                f'• 输入"取消"退出',
                parse_mode='markdown'
            )
            return

        # 主菜单命令处理
        if is_cmd(text, '监听关键词'):
            await start_bulk_keywords(event, 'listen')
            return

        if is_cmd(text, '点击关键词'):
            await start_bulk_keywords(event, 'click')
            return

        if is_cmd(text, '设置目标机器人'):
            # 先清理数据库中可能的重复数据
            from storage import dao_settings
            dao_settings.cleanup_duplicate_global_settings()
            cur = settings_service.get_target_bot()
            cur_disp = ('@' + cur) if cur else '（未设置）'
            set_state(chat_id, 'set_target_bot')
            await event.respond(
                '🎯 设置目标机器人\n'
                f'当前目标机器人：{cur_disp}\n\n'
                '⚠️ 请直接输入机器人用户名：\n'
                '• 格式：@botname 或 botname\n'
                '• 例如：@uy07bot 或 uy07bot\n'
                '• 输入"取消"退出',
                buttons=None
            )
            return

        if is_cmd(text, '账号列表'):
            rows = dao_accounts.list_all()
            if not rows:
                await event.respond('📭 暂无账号')
                return

            listen_rows = list_accounts('listen')
            click_rows = list_accounts('click')
            def format_rows(items):
                if not items:
                    return '（无）'
                result = []
                for r in items:
                    role = get_account_role(r['id'])
                    role_label = format_role_label(role)
                    ident = r['username'] or r['phone'] or f"#{r['id']}"
                    result.append(f"• #{r['id']} {ident} ({role_label}, {r['status']})")
                return '\n'.join(result)
            summary = (
                f"📒 账号列表（共 {len(rows)} 个）\n\n"
                f"监听账号（{len(listen_rows)}）：\n{format_rows(listen_rows)}\n\n"
                f"点击账号（{len(click_rows)}）：\n{format_rows(click_rows)}"
            )
            await event.respond(summary)
            return


        # 诊断功能：列出账号加入的所有群组，或检查特定群组
        if text.startswith('诊断群组') or text.startswith('诊断 #'):
            # 支持两种格式：
            # 1. 诊断群组 #账号ID - 列出所有群组
            # 2. 诊断群组 #账号ID -1002964498071 - 检查特定群组
            match = re.search(r'#(\d+)', text)
            target_chat_id = None
            chat_id_match = re.search(r'-?\d{10,}', text)
            if chat_id_match:
                target_chat_id = int(chat_id_match.group(0))
            
            if match:
                account_id = int(match.group(1))
                client = manager.account_clients.get(account_id)
                if not client:
                    await event.respond(f'❌ 账号 #{account_id} 未在线')
                return

                if target_chat_id:
                    # 检查特定群组
                    await event.respond(f'🔍 正在检查群组 {target_chat_id}，请稍候...')
                    try:
                        # 尝试获取群组实体
                        try:
                            entity = await client.get_entity(target_chat_id)
                            chat_title = getattr(entity, 'title', '') or getattr(entity, 'username', '') or f"Chat#{target_chat_id}"
                            chat_username = getattr(entity, 'username', None)
                            is_megagroup = getattr(entity, 'megagroup', False)
                            is_broadcast = getattr(entity, 'broadcast', False)
                            chat_type = "超级群组" if is_megagroup else ("频道" if is_broadcast else "群组")
                            
                            # 检查账号是否在群组中
                            try:
                                await client.get_participants(entity, limit=1)
                                is_member = True
                            except:
                                is_member = False
                            
                            result = (
                                f'📊 **群组诊断结果**\n\n'
                                f'**群组信息：**\n'
                                f'• 名称：{chat_title}\n'
                                f'• Chat ID：`{target_chat_id}`\n'
                                f'• 用户名：@{chat_username if chat_username else "无"}\n'
                                f'• 类型：{chat_type}\n'
                                f'• 是超级群组：{"是" if is_megagroup else "否"}\n'
                                f'• 是广播频道：{"是" if is_broadcast else "否"}\n\n'
                                f'**账号状态：**\n'
                                f'• 账号 #{account_id} {"✅ 已加入" if is_member else "❌ 未加入或无法访问"}\n\n'
                                f'**监听状态：**\n'
                                f'• 会被监听：{"✅ 是" if (is_megagroup and not is_broadcast) or (not is_broadcast) else "❌ 否（可能是广播频道）"}'
                            )
                            await event.respond(result, parse_mode='markdown')
                        except Exception as e:
                            await event.respond(f'❌ 无法获取群组信息：{str(e)}\n\n可能原因：\n• 账号未加入该群组\n• 群组ID错误\n• 没有访问权限')
                        return
                    except Exception as e:
                        await event.respond(f'❌ 诊断失败：{str(e)}')
            return

            # 列出所有群组
            await event.respond('🔍 正在获取群组列表，请稍候...')
            try:
                groups = []
                async for dialog in client.iter_dialogs():
                    if not dialog.is_user:  # 只获取群组和频道
                        chat = dialog.entity
                        chat_id = chat.id
                        chat_title = getattr(chat, 'title', '') or getattr(chat, 'username', '') or f"Chat#{chat_id}"
                        chat_username = getattr(chat, 'username', None)
                        is_megagroup = getattr(chat, 'megagroup', False)
                        is_broadcast = getattr(chat, 'broadcast', False)
                        chat_type = "超级群组" if is_megagroup else ("频道" if is_broadcast else "群组")
                        groups.append({
                            'title': chat_title,
                            'id': chat_id,
                            'username': chat_username,
                            'type': chat_type
                        })
                
                if not groups:
                    await event.respond(f'⚠️ 账号 #{account_id} 未加入任何群组或频道')
                return
                
                # 按类型分组显示
                groups_by_type = {}
                for g in groups:
                    gtype = g['type']
                    if gtype not in groups_by_type:
                        groups_by_type[gtype] = []
                    groups_by_type[gtype].append(g)
                
                result = f"📊 账号 #{account_id} 的群组列表（共 {len(groups)} 个）\n\n"
                for gtype in ['超级群组', '频道', '群组']:
                    if gtype in groups_by_type:
                        result += f"**{gtype}** ({len(groups_by_type[gtype])} 个):\n"
                        for g in groups_by_type[gtype][:20]:  # 每种类型最多显示20个
                            username_str = f" @{g['username']}" if g['username'] else ""
                            result += f"• {g['title']}{username_str} (ID: {g['id']})\n"
                        if len(groups_by_type[gtype]) > 20:
                            result += f"  ... 还有 {len(groups_by_type[gtype]) - 20} 个\n"
                        result += "\n"
                
                await event.respond(result, parse_mode='markdown')
            except Exception as e:
                    await event.respond(f'❌ 获取群组列表失败: {str(e)}')
                    import traceback
                    traceback.print_exc()
            else:
                await event.respond(
                    '⚠️ 请使用格式：\n'
                    '• `诊断群组 #账号ID` - 列出所有群组\n'
                    '• `诊断群组 #账号ID -1002964498071` - 检查特定群组\n\n'
                    '例如：\n'
                    '• 诊断群组 #5\n'
                    '• 诊断群组 #5 -1002964498071',
                    parse_mode='markdown'
                )
            return

        if is_cmd(text, '移除所有账号'):
            print(f"[移除账号] 收到命令: 移除所有账号")
            try:
                buttons = [
                    [Button.inline('移除监听账号', data='remove_all_role:listen')],
                    [Button.inline('移除点击账号', data='remove_all_role:click')],
                    [Button.inline('移除全部账号', data='remove_all_role:all')],
                    [Button.inline('取消', data='remove_all_role:cancel')]
                ]
                await event.respond(
                    '⚠️ 请选择要移除的账号类型：',
                    buttons=buttons
                )
                print(f"[移除账号] 已发送选择按钮")
            except Exception as e:
                print(f"[移除账号] ❌ 发送消息失败: {e}")
                import traceback
                traceback.print_exc()
                await event.respond(f'❌ 发送消息失败：{e}')
            return

        if is_cmd(text, '添加监听账号'):
            # 检查是否已经在添加账号状态，避免重复提示
            current_st = get_state(chat_id)
            if current_st and current_st.get('mode') == 'add_listen_account_wait_string':
                await event.respond('⚠️ 您已经在添加监听账号模式中，请发送 StringSession 文本或 .session 文件，或发送"完成"结束添加')
            return

            set_state(chat_id, 'add_listen_account_wait_string')
            await event.respond(
                '🔑 添加监听账号（可连续）\n'
                '• 发送 StringSession 文本 或 .session 文件（作为文档）进行添加\n'
                '• 发送"完成"结束添加\n'
                '提示：StringSession 通常以 1A 开头'
            )
            return

        if is_cmd(text, '添加点击账号'):
            # 检查是否已经在添加账号状态，避免重复提示
            current_st = get_state(chat_id)
            if current_st and current_st.get('mode') == 'add_click_account_wait_file':
                await event.respond('⚠️ 您已经在添加点击账号模式中，请发送 StringSession 文本或 .session 文件，或发送"完成"结束添加')
                return
            
            set_state(chat_id, 'add_click_account_wait_file')
            await event.respond(
                '🖱️ 添加点击账号（可连续）\n'
                '• 发送 .session 文件（作为文档）或 StringSession 文本进行添加\n'
                '• 发送"完成"结束添加\n'
                '提示：StringSession 通常以 1A 开头'
            )
            return

        if is_cmd(text, '设置点击延迟'):
            rows = list_accounts('click')
            if not rows:
                await event.respond('⚠️ 尚无点击账号，请先添加。')
                return
            acc_hint = extract_account_id(text)
            target_id = None
            if acc_hint and dao_accounts.get(acc_hint):
                if role_allows_click(get_account_role(acc_hint)):
                    target_id = acc_hint
            elif len(rows) == 1:
                target_id = rows[0]['id']
            if target_id:
                set_state(chat_id, 'set_click_delay_input', account_id=target_id)
                await event.respond('⏱️ 请输入点击延迟（单位秒，可为小数，例如 0.8）')
            else:
                set_state(chat_id, 'set_click_delay_choose_account')
                listing = '\n'.join([f"{r['id']}: {r['username'] or r['phone'] or ''}" for r in rows])
                await event.respond('🔢 请输入要设置点击延迟的账号ID：\n' + listing)
            return

        # 发送相关功能入口（全局设置）
        if is_cmd(text, '设置发送消息'):
            cur = settings_service.get_global_template() or '（未设置，默认 /start）'
            set_state(chat_id, 'set_global_template')
            await event.respond(
                f'📝 设置发送消息\n'
                f'当前消息：{cur}\n\n'
                '请输入要发送的消息内容：\n'
                '（所有点击账号将使用此消息）'
            )
            return

        if is_cmd(text, '设置发送延迟'):
            cur = settings_service.get_global_send_delay()
            set_state(chat_id, 'set_global_send_delay')
            await event.respond(
                f'🐢 设置发送延迟\n'
                f'当前延迟：{cur} 秒\n\n'
                '请输入发送延迟（单位秒，可为小数）：\n'
                '（每个账号发送后等待的时间）'
            )
            return

        if is_cmd(text, '开始发送'):
            rows = list_accounts('click')
            if not rows:
                await event.respond('⚠️ 尚无点击账号，请先添加。')
                return
            
            # 获取目标机器人
            bot_username = settings_service.get_target_bot()
            if not bot_username:
                await event.respond('⚠️ 请先设置目标机器人（点击"🎯 设置目标机器人"）', buttons=main_keyboard())
            return

            # 获取发送消息（默认 /start）
            send_msg = settings_service.get_global_template() or '/start'
            # 获取发送延迟
            send_delay = settings_service.get_global_send_delay()
            
            target = f"@{bot_username}"
            click_accounts = [acc_id for acc_id, client in list(manager.account_clients.items()) if role_allows_click(get_account_role(acc_id))]
            if not click_accounts:
                await event.respond('⚠️ 当前没有激活的点击账号，无法发送消息', buttons=main_keyboard())
                return
            
            # 开启所有点击账号的发送开关
            for r in rows:
                settings_service.set_start_sending(True, r['id'])
            
            # 发送消息
            await event.respond(f'⏳ 正在发送，共 {len(click_accounts)} 个账号…')
            ok = 0
            fail_details = []
            for i, acc_id in enumerate(click_accounts):
                client = manager.account_clients.get(acc_id)
                if not client:
                    acc_info = dao_accounts.get(acc_id)
                    acc_label = acc_info.get('username') or acc_info.get('phone') or f"#{acc_id}"
                    fail_details.append(f"账号 {acc_label}: 客户端未连接")
                    continue
                try:
                    await client.send_message(target, send_msg)
                    ok += 1
                except Exception as e:
                    acc_info = dao_accounts.get(acc_id)
                    acc_label = acc_info.get('username') or acc_info.get('phone') or f"#{acc_id}"
                    fail_details.append(f"账号 {acc_label}: {str(e)}")
                
                # 发送延迟（最后一个账号不需要等待）
                if send_delay > 0 and i < len(click_accounts) - 1:
                    await asyncio.sleep(send_delay)
            
            msg_parts = [
                f"✅ 发送完成（共 {len(click_accounts)} 个账号）",
                f"\n📝 发送消息：{send_msg}",
                f"🎯 目标用户：{target}",
                f"🐢 发送延迟：{send_delay} 秒",
                f"\n✅ 成功：{ok} 个"
            ]
            if fail_details:
                msg_parts.append(f"❌ 失败：{len(fail_details)} 个")
                msg_parts.append("\n失败详情：")
                for detail in fail_details[:10]:
                    msg_parts.append(f"• {detail}")
                if len(fail_details) > 10:
                    msg_parts.append(f"• ... 还有 {len(fail_details) - 10} 个失败")
            
            msg = '\n'.join(msg_parts)
            await event.respond(msg, buttons=main_keyboard())
            return

        if is_cmd(text, '自动进群'):
            listen_active = [r['id'] for r in list_accounts('listen') if r['id'] in manager.account_clients]
            click_active = [r['id'] for r in list_accounts('click') if r['id'] in manager.account_clients]
            buttons = []
            if listen_active:
                buttons.append([Button.inline('监听账号进群', data='auto_join:listen')])
            if click_active:
                buttons.append([Button.inline('点击账号进群', data='auto_join:click')])
            if not buttons:
                await event.respond('⚠️ 当前没有已连接的账号，请先确保账号在线。')
                return
            await event.respond('请选择要用于自动进群的账号类型：', buttons=buttons)
            return

        return

    @bot.on(events.NewMessage(func=lambda e: e.file and e.is_private))
    async def _(event):
        # 仅在等待添加账号或关键词导入时接收文件
        chat_id = event.chat_id
        st = get_state(chat_id)
        if not st or st['mode'] not in (
            'add_account_wait_file',
            'add_click_account_wait_file',
            'add_listen_account_wait_string',
            'keywords_import_wait_file',
        ):
            return
        try:
            doc = event.document
            name = doc.attributes[0].file_name if doc.attributes else 'session.session'
            if st['mode'] in ('add_account_wait_file', 'add_click_account_wait_file', 'add_listen_account_wait_string'):
                tmp_path = os.path.join('sessions', f'_upload_{event.id}_{name}')
                os.makedirs('sessions', exist_ok=True)
                await event.download_media(file=tmp_path)
                final_path = sess_service.save_session_file(tmp_path, name)
                info = await manager.add_account_from_session_file(final_path)
                account_id = info['id']
                # auto-assign role based on entry
                if st['mode'] == 'add_click_account_wait_file':
                    # 如果账号已存在，合并角色；否则设置为 click
                    current_role = settings_service.get_account_role(account_id) or 'both'
                    if info.get('existing', False):
                        # 账号已存在，合并角色
                        if current_role == 'listen':
                            settings_service.set_account_role(account_id, 'both')
                            # 自动应用全局点击关键词
                            settings_service.apply_global_click_keywords_to_account(account_id)
                            role_msg = "（角色已合并为：监听+点击，已应用全局点击关键词）"
                        elif current_role == 'click':
                            role_msg = "（角色保持为：点击）"
                        else:
                            role_msg = f"（角色：{format_role_label(current_role)}）"
                    else:
                        # 新账号，设置为 click
                        settings_service.set_account_role(account_id, 'click')
                        # 自动应用全局点击关键词
                        settings_service.apply_global_click_keywords_to_account(account_id)
                        role_msg = "（角色：点击，已应用全局点击关键词）"
                    # stay in continuous add mode
                    await event.respond(
                        f"✅ 点击账号添加成功！\n用户昵称：{info.get('nickname') or ''}\n用户名：{info.get('username') or '无'}\n账号：{info.get('phone') or ''}\n{role_msg}\n\n继续添加：再发送文件或 StringSession 文本\n结束：发送「完成」"
                    )
                elif st['mode'] == 'add_listen_account_wait_string':
                    # 如果账号已存在，合并角色；否则设置为 listen
                    current_role = settings_service.get_account_role(account_id) or 'both'
                    if info.get('existing', False):
                        # 账号已存在，合并角色
                        if current_role == 'click':
                            settings_service.set_account_role(account_id, 'both')
                            # 自动应用全局点击关键词
                            settings_service.apply_global_click_keywords_to_account(account_id)
                            role_msg = "（角色已合并为：监听+点击，已应用全局点击关键词）"
                        elif current_role == 'listen':
                            role_msg = "（角色保持为：监听）"
                        else:
                            role_msg = f"（角色：{format_role_label(current_role)}）"
                    else:
                        # 新账号，设置为 listen
                        settings_service.set_account_role(account_id, 'listen')
                        role_msg = "（角色：监听）"
                    # stay in continuous add mode
                    await event.respond(
                        f"✅ 监听账号添加成功！\n用户昵称：{info.get('nickname') or ''}\n用户名：{info.get('username') or '无'}\n账号：{info.get('phone') or ''}\n{role_msg}\n\n继续添加：再发送文件或 StringSession 文本\n结束：发送「完成」\n（提醒目标可稍后在菜单中为该账号设置）"
                    )
                else:
                    # legacy path: fallback to choose role
                    set_state(chat_id, 'choose_account_role', account_id=info['id'])
                    await event.respond(
                        f"✅ 账号添加成功！\n用户昵称：{info.get('nickname') or ''}\n用户名：{info.get('username') or '无'}\n账号：{info.get('phone') or ''}\n\n请选择该账号的角色：",
                        buttons=roles_keyboard()
                    )
            elif st['mode'] == 'keywords_import_wait_file':
                # 从文本文件导入关键字（追加模式）
                account_id = st['pending']['account_id']
                kind = st['pending']['kind']

                tmp_dir = 'tmp_import'
                os.makedirs(tmp_dir, exist_ok=True)
                tmp_path = os.path.join(tmp_dir, f'kw_{event.id}_{name}')
                await event.download_media(file=tmp_path)

                words = []
                try:
                    with open(tmp_path, 'r', encoding='utf-8') as f:
                        for line in f:
                            line = (line or '').strip()
                            if not line:
                                continue
                            # 支持一行多个，逗号/顿号分隔
                            parts = (
                                line.replace('，', ',')
                                .replace('、', ',')
                                .split(',')
                            )
                            for p in parts:
                                p = (p or '').strip()
                                if p:
                                    words.append(p)
                finally:
                    try:
                        os.remove(tmp_path)
                    except OSError:
                        pass

                before = set(settings_service.get_account_keywords(account_id, kind=kind) or [])
                # 覆盖式导入：在原有基础上追加去重
                for w in words:
                    settings_service.add_keyword(account_id, w, kind=kind)
                after = set(settings_service.get_account_keywords(account_id, kind=kind) or [])
                added = len(after - before)

                set_state(chat_id, 'keywords_manage', account_id=account_id, kind=kind)
                cur = settings_service.get_account_keywords(account_id, kind=kind) or []
                await event.respond(
                    f"📥 关键字导入完成（{keywords_label(kind)}）\n"
                    f"本次新增：{added} 条，当前总数：{len(cur)} 条"
                )
                await event.respond(keywords_overview_text(account_id, kind))
        except Exception as e:
            set_state(chat_id)
            await event.respond(f"文件处理失败：{e}", buttons=main_keyboard())
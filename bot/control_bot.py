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
        buttons.append([Button.inline('监听关键字', data=f'acc|{acc_id}|kwl'),
                        Button.inline('监听群组', data=f'acc|{acc_id}|lsrc')])
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


async def setup_handlers(manager: ClientManager):
    bot = manager.bot

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
        await event.respond(
            f"📢 批量添加 {keywords_label(kind)} 关键字\n\n"
            f"当前关键词（共 {len(rows)} 个账号，{total_keywords} 个关键词）：\n{current_status}\n\n"
            "操作说明：\n"
            "• 发送关键字列表（换行/逗号分隔）将追加到所有对应账号\n"
            "• 发送 “清空” 将删除所有对应账号的该类关键字\n"
            "• 发送 “完成” 返回主菜单"
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
            '• 🚪 自动进群\n\n'
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
        action = event.pattern_match.group(1).decode()
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
        if not targets:
            await event.answer('暂无可移除账号', alert=True)
            return
        await event.answer('⏳ 正在移除…')
        count = 0
        for r in targets:
            await remove_account(r['id'])
            count += 1
        msg = f"🗑️ 已移除 {label} 账号 {count} 个。"
        try:
            await event.edit(msg, buttons=None)
        except Exception:
            await bot.send_message(event.chat_id, msg)

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
        if action == 'lsrc':
            if not role_allows_listen(role):
                await event.answer('该账号不是监听账号', alert=True)
                return
            set_state(event.chat_id, 'listen_sources_manage', account_id=acc_id)
            cur = settings_service.get_listen_sources(acc_id) or []
            preview = '\n'.join(['• ' + x for x in cur[:20]]) or '（空）'
            await bot.send_message(
                event.chat_id,
                f"📡 监听群组（共 {len(cur)} 条，预览前20条）：\n{preview}\n\n"
                '新增：直接发送（可多行）\n删除：发送 q值\n导入：发送“导入”上传文本文件\n导出、清空、完成亦可发送对应指令'
            )
            await event.answer('请在聊天中继续操作')
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

    async def start_click_job(manager: ClientManager, target_chat_id, target_msg_id, accounts: List[dict], report_chat_id: int):
        """开始点击任务：获取消息、匹配关键词并依次点击"""
        bot = manager.bot
        try:
            # 获取消息（使用bot客户端先获取）
            try:
                target_msg = await bot.get_messages(target_chat_id, ids=target_msg_id)
                if not target_msg:
                    await bot.send_message(report_chat_id, f'❌ 无法获取消息（ID: {target_msg_id}）')
                    return
            except Exception as e:
                await bot.send_message(report_chat_id, f'❌ 获取消息失败：{e}')
                return
            
            # 检查消息是否有按钮
            buttons = getattr(target_msg, 'buttons', None)
            if not buttons:
                await bot.send_message(report_chat_id, '⚠️ 该消息没有按钮')
                return
            
            # 收集所有按钮文本和位置
            button_positions = []  # [(row, col, text), ...]
            for i, row in enumerate(buttons):
                for j, btn in enumerate(row):
                    btn_text = getattr(btn, 'text', None) or ''
                    button_positions.append((i, j, btn_text))
            
            # 检查哪些账号有关键词匹配
            matched_accounts = []
            for acc in accounts:
                acc_id = acc['id']
                keywords = settings_service.get_account_keywords(acc_id, kind='click') or []
                if not keywords:
                    continue
                # 检查是否有按钮包含关键词
                for i, j, btn_text in button_positions:
                    if any(k for k in keywords if k and k in btn_text):
                        matched_accounts.append((acc, i, j, btn_text))
                        break  # 每个账号只匹配第一个按钮
            
            if not matched_accounts:
                all_btn_texts = [bt[2] for bt in button_positions]
                await bot.send_message(
                    report_chat_id,
                    f'⚠️ 没有账号的关键词匹配到按钮\n\n'
                    f'按钮文本：{", ".join(all_btn_texts[:5])}{"..." if len(all_btn_texts) > 5 else ""}'
                )
                return
            
            # 发送开始报告
            all_btn_texts = [bt[2] for bt in button_positions]
            await bot.send_message(
                report_chat_id,
                f'🚀 **开始点击任务**\n'
                f'━━━━━━━━━━━━━━━━\n'
                f'📱 匹配账号数：{len(matched_accounts)}\n'
                f'📋 按钮文本：{", ".join(all_btn_texts[:3])}{"..." if len(all_btn_texts) > 3 else ""}',
                parse_mode='markdown'
            )
            
            # 依次使用每个账号点击
            success_count = 0
            fail_count = 0
            for idx, (acc, btn_row, btn_col, btn_text) in enumerate(matched_accounts):
                acc_id = acc['id']
                acc_name = acc.get('username') or acc.get('phone') or f"#{acc_id}"
                
                # 获取账号客户端
                client = manager.account_clients.get(acc_id)
                if not client:
                    fail_count += 1
                    await bot.send_message(report_chat_id, f'❌ 账号 {acc_name} 离线，跳过')
                    continue
                
                try:
                    # 获取点击延迟
                    delay = settings_service.get_click_delay(acc_id) or 0
                    if delay > 0:
                        await asyncio.sleep(delay)
                    
                    # 获取消息（使用账号客户端）
                    try:
                        acc_msg = await client.get_messages(target_chat_id, ids=target_msg_id)
                        if not acc_msg:
                            raise Exception('无法获取消息')
                    except Exception as e:
                        fail_count += 1
                        await bot.send_message(report_chat_id, f'❌ 账号 {acc_name} 无法获取消息：{e}')
                        continue
                    
                    # 点击按钮
                    try:
                        await acc_msg.click(btn_row, btn_col)
                        success_count += 1
                        await bot.send_message(report_chat_id, f'✅ 账号 {acc_name} 点击成功（按钮：{btn_text}）')
                    except Exception as e:
                        fail_count += 1
                        await bot.send_message(report_chat_id, f'❌ 账号 {acc_name} 点击失败：{e}')
                    
                    # 每个账号间隔3秒
                    if idx < len(matched_accounts) - 1:
                        await asyncio.sleep(3)
                        
                except Exception as e:
                    fail_count += 1
                    await bot.send_message(report_chat_id, f'❌ 账号 {acc_name} 处理失败：{e}')
            
            # 发送完成报告
            await bot.send_message(
                report_chat_id,
                f'✅ **点击任务完成**\n'
                f'━━━━━━━━━━━━━━━━\n'
                f'✅ 成功：{success_count} 个\n'
                f'❌ 失败：{fail_count} 个',
                parse_mode='markdown'
            )
        except Exception as e:
            await bot.send_message(report_chat_id, f'❌ 点击任务出错：{e}')
    
    @bot.on(events.NewMessage)
    async def _(event):
        chat_id = event.chat_id
        text = (event.raw_text or '').strip()
        st = get_state(chat_id)

        # 如果在 set_target_bot 模式下且输入包含 emoji，直接拒绝（可能是按钮点击）
        if st and st.get('mode') == 'set_target_bot':
            if any(unicodedata.category(c) == 'So' for c in text):
                await event.respond('⚠️ 请直接输入用户名，不要点击按钮', buttons=None)
                return
        
        # 主菜单按钮文本
        MAIN_MENU_COMMANDS = {
            '🧩 监听关键词', '🧩 点击关键词',
            '📒 账号列表', '▶️ 开始点击',
            '➕ 添加监听账号', '➕ 添加点击账号',
            '📡 设置监听群组', '📤 设置转发目标',
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
        
        if is_main_menu_cmd:
            # 如果是主菜单命令，直接清除状态，让后续的命令处理器接管
            set_state(chat_id, None)
            st = None

        # 进行中的状态优先
        if st:
            mode = st['mode']
            if mode == 'bulk_keywords_input':
                kind = st['pending']['kind']
                t = (text or '').strip()
                rows = list_accounts('listen' if kind == 'listen' else 'click')
                if not rows:
                    set_state(chat_id)
                    await event.respond('⚠️ 当前没有可用账号，请先添加。', buttons=main_keyboard())
                    return
                if t in ('完成', '返回'):
                    set_state(chat_id)
                    await event.respond('✅ 已返回主菜单', buttons=main_keyboard())
                    return
                if t.lower() in ('清空', 'clear'):
                    for r in rows:
                        dao_keywords.set_keywords(r['id'], [], kind=kind)
                    set_state(chat_id)
                    await event.respond(f"🧹 已清空 {len(rows)} 个{keywords_label(kind)}账号的关键字", buttons=main_keyboard())
                    return
                parts = split_keywords_payload(t)
                if not parts:
                    await event.respond('⚠️ 请发送关键字内容，或发送"完成"返回主菜单。')
                    return
                for r in rows:
                    for word in parts:
                        settings_service.add_keyword(r['id'], word, kind=kind)
                set_state(chat_id)
                await event.respond(
                    f"✅ 已为 {len(rows)} 个{keywords_label(kind)}账号追加 {len(parts)} 条关键字",
                    buttons=main_keyboard()
                )
                return
            if mode == 'choose_account_role':
                account_id = st['pending']['account_id']
                t = text.strip()
                if t in ('监听账号', '监听', 'listen'):
                    settings_service.set_account_role(account_id, 'listen')
                    set_state(chat_id, 'set_account_target', account_id=account_id)
                    await event.respond('🎯 该账号为“监听账号”。请输入此账号的提醒目标（chat_id 或 @username）。\n提示：留空或发送“全局”将使用全局目标。')
                    return
                if t in ('点击账号', '点击', 'click'):
                    settings_service.set_account_role(account_id, 'click')
                    set_state(chat_id)
                    await event.respond('✅ 已设置为“点击账号”', buttons=main_keyboard())
                    return
                if t in ('同时监听与点击', 'both'):
                    settings_service.set_account_role(account_id, 'both')
                    set_state(chat_id, 'set_account_target', account_id=account_id)
                    await event.respond('🎯 该账号为“同时”。请输入此账号的提醒目标（chat_id 或 @username）。\n提示：留空或发送“全局”将使用全局目标。')
                    return
                if t in ('跳过', 'skip'):
                    set_state(chat_id)
                    await event.respond('已跳过角色设置（默认按全局策略处理）', buttons=main_keyboard())
                    return
                await event.respond('请选择账号角色：', buttons=roles_keyboard())
                return
            if mode == 'set_account_target':
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
            if mode == 'set_forward_target_choose_account':
                try:
                    acc_id = int(text)
                    row = dao_accounts.get(acc_id)
                    if not row:
                        await event.respond('⚠️ 账号不存在，请重新输入账号ID')
                        return
                    if not role_allows_listen(get_account_role(acc_id)):
                        await event.respond('⚠️ 该账号不是监听账号，请重新输入监听账号ID')
                        return
                    cur = settings_service.get_account_target_chat(acc_id) or settings_service.get_target_chat() or '（未设置）'
                    set_state(chat_id, 'set_forward_target', account_id=acc_id)
                    await event.respond(
                        f'📤 设置转发目标（账号 #{acc_id}）\n'
                        f'当前转发目标：{cur}\n\n'
                        '请输入转发目标：\n'
                        '• 用户名：@username\n'
                        '• 群组/频道：@groupname 或 chat_id\n'
                        '• 链接：https://t.me/username\n'
                        '• 输入"全局"使用全局设置\n'
                        '• 输入"清空"清除账号专属设置\n'
                        '• 输入"取消"退出'
                    )
                except Exception:
                    await event.respond('⚠️ 请输入有效的账号ID（数字）')
                return
            if mode == 'set_forward_target':
                account_id = st['pending']['account_id']
                t = (text or '').strip()
                if t in ('取消', '退出', 'cancel'):
                    set_state(chat_id)
                    await event.respond('✅ 已取消', buttons=main_keyboard())
                    return
                if t in ('清空', 'clear'):
                    settings_service.set_account_target_chat(account_id, None)
                    set_state(chat_id)
                    await event.respond('✅ 已清空转发目标', buttons=main_keyboard())
                    return
                # 处理输入：支持 @username, chat_id, https://t.me/username
                clean_target = t.strip()
                if clean_target.startswith('http://') or clean_target.startswith('https://'):
                    clean_target = clean_target.rsplit('/', 1)[-1]
                if clean_target.startswith('@'):
                    clean_target = clean_target[1:]
                settings_service.set_account_target_chat(account_id, clean_target if clean_target else t)
                set_state(chat_id)
                await event.respond(f'✅ 转发目标已设置：{clean_target if clean_target else t}', buttons=main_keyboard())
                return
            if mode == 'set_target_chat':
                settings_service.set_target_chat(text)
                set_state(chat_id)
                await event.respond('已设置提醒目标', buttons=main_keyboard())
                return
            if mode == 'start_click_wait_link':
                t = (text or '').strip()
                if t in ('取消', '退出', 'cancel'):
                    set_state(chat_id)
                    await event.respond('✅ 已取消', buttons=main_keyboard())
                    return
                
                # 解析消息链接
                # 支持格式：https://t.me/c/xxx/123 或 https://t.me/username/123
                msg_link = t
                chat_id_from_link = None
                msg_id_from_link = None
                
                # 解析 t.me/c/xxx/123 格式（超级群组/频道）
                match1 = re.search(r't\.me/c/(\d+)/(\d+)', msg_link)
                if match1:
                    channel_id = match1.group(1)
                    msg_id_from_link = int(match1.group(2))
                    # 转换为 -100xxxxxxxxxx 格式
                    chat_id_from_link = int(f'-100{channel_id}')
                else:
                    # 解析 t.me/username/123 格式
                    match2 = re.search(r't\.me/([a-zA-Z0-9_]+)/(\d+)', msg_link)
                    if match2:
                        username = match2.group(1)
                        msg_id_from_link = int(match2.group(2))
                        chat_id_from_link = username
                
                if not chat_id_from_link or not msg_id_from_link:
                    await event.respond('⚠️ 消息链接格式无效，请发送类似 https://t.me/c/xxx/123 或 https://t.me/username/123 的链接')
                    return
                
                # 获取所有点击账号
                click_accounts = list_accounts('click')
                if not click_accounts:
                    set_state(chat_id)
                    await event.respond('⚠️ 没有可用的点击账号', buttons=main_keyboard())
                    return
                
                set_state(chat_id)
                await event.respond(
                    f'✅ 已解析消息链接，准备为 {len(click_accounts)} 个点击账号依次执行点击操作。',
                    buttons=main_keyboard()
                )
                
                # 异步执行点击任务
                asyncio.create_task(start_click_job(
                    manager, chat_id_from_link, msg_id_from_link, click_accounts, event.chat_id
                ))
                return
            if mode == 'set_target_bot':
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
                    await event.respond('⚠️ 请输入正确的用户名（不含emoji）', buttons=None)
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
            if mode == 'set_global_template':
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
            if mode == 'set_global_send_delay':
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
            if mode == 'add_account_wait_file':
                await event.respond('请发送 .session 文件作为文档（不是文本）')
                return
            if mode == 'add_listen_account_wait_string':
                # 如果消息包含文件，让文件处理器处理，不在这里处理
                if event.file:
                    return
                tmsg = (text or '').strip()
                if tmsg in ('完成', '结束', '返回'):
                    set_state(chat_id)
                    await event.respond('✅ 已结束添加', buttons=main_keyboard())
                    return
                session_str = tmsg
                if not session_str:
                    await event.respond('⚠️ 请输入有效的 StringSession 文本，或发送 .session 文件（作为文档）')
                    return
                try:
                    info = await manager.add_account_from_string_session(session_str)
                    settings_service.set_account_role(info['id'], 'listen')
                    # 保持在连续添加模式
                    await event.respond(
                        f"✅ 监听账号添加成功！\n用户昵称：{info.get('nickname') or ''}\n用户名：{info.get('username') or '无'}\n账号：{info.get('phone') or ''}\n\n继续添加：发送 StringSession 文本或 .session 文件\n结束：发送“完成”\n（提醒目标可稍后在菜单中为该账号设置）"
                    )
                except Exception as e:
                    await event.respond(f"⚠️ 解析为 StringSession 失败：{e}\n也可以直接发送 .session 文件（作为文档）来添加。")
                return
            if mode == 'add_click_account_wait_file':
                # 如果消息包含文件，让文件处理器处理，不在这里处理
                if event.file:
                    return
                # 也支持文本 StringSession，作为点击账号
                t = (text or '').strip()
                if t in ('完成', '结束', '返回'):
                    set_state(chat_id)
                    await event.respond('✅ 已结束添加', buttons=main_keyboard())
                    return
                try:
                    info = await manager.add_account_from_string_session(t)
                    settings_service.set_account_role(info['id'], 'click')
                    # 保持在连续添加模式
                    await event.respond(
                        f"✅ 点击账号添加成功！\n用户昵称：{info.get('nickname') or ''}\n用户名：{info.get('username') or '无'}\n账号：{info.get('phone') or ''}\n\n继续添加：发送 StringSession 文本或 .session 文件\n结束：发送“完成”"
                    )
                except Exception as e:
                    await event.respond(f"⚠️ 解析为 StringSession 失败：{e}\n也可以发送 .session 文件（作为文档）来添加点击账号。")
                return
            if mode == 'keywords_manage':
                account_id = st['pending']['account_id']
                kind = st['pending']['kind']
                t = (text or '').strip()
                if not t:
                    await event.respond('⚠️ 请发送指令，或发送“完成”返回主菜单。')
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
            if mode == 'set_click_delay_choose_account':
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
            if mode == 'set_click_delay_input':
                account_id = st['pending']['account_id']
                try:
                    value = float(text)
                    settings_service.set_click_delay(str(value), account_id)
                    set_state(chat_id)
                    await event.respond('✅ 已设置点击延迟', buttons=main_keyboard())
                except Exception:
                    await event.respond('⚠️ 请输入数字，例如 0.8')
                return
            if mode == 'set_send_delay_choose_account':
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
            if mode == 'set_send_delay_input':
                account_id = st['pending']['account_id']
                try:
                    value = float(text)
                    settings_service.set_send_delay(str(value), account_id)
                    set_state(chat_id)
                    await event.respond('✅ 已设置发送延迟', buttons=main_keyboard())
                except Exception:
                    await event.respond('⚠️ 请输入数字，例如 1.2')
                return
            if mode == 'set_template_choose_account':
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
            if mode == 'set_template_input':
                account_id = st['pending']['account_id']
                settings_service.set_template_message(text, account_id)
                set_state(chat_id)
                await event.respond('✅ 已设置发送消息模板', buttons=main_keyboard())
                return
            if mode == 'auto_join_wait_link':
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

            if mode == 'set_listen_sources_choose_account':
                try:
                    acc_id = int(text)
                    row = dao_accounts.get(acc_id)
                    if not row:
                        await event.respond('⚠️ 账号不存在，请重新输入账号ID')
                        return
                    if not role_allows_listen(get_account_role(acc_id)):
                        await event.respond('⚠️ 该账号不是监听账号，请重新输入监听账号ID')
                        return
                    set_state(chat_id, 'listen_sources_manage', account_id=acc_id)
                    cur = settings_service.get_listen_sources(acc_id) or []
                    preview = '\n'.join(['• ' + x for x in cur[:20]]) or '（空）'
                    await event.respond(
                        f"📡 监听群组（共 {len(cur)} 条，预览前20条）：\n{preview}\n\n"
                        '新增：直接发送 @group / 123456789 / https://t.me/xxx（支持多行）\n'
                        '删除：发送 q值（例：q@group / q123456）\n'
                        '导入：发送“导入”，上传文本文件（每行一个）\n'
                        '导出：发送“导出”\n'
                        '清空：发送“清空”\n'
                        '返回：发送“完成”'
                    )
                except Exception:
                    await event.respond('⚠️ 请输入有效的账号ID（数字）')
                return
            if mode == 'listen_sources_manage':
                acc_id = st['pending']['account_id']
                t = (text or '').strip()
                if t in ('完成', '返回'):
                    set_state(chat_id)
                    await event.respond('⬅️ 已返回主菜单', buttons=main_keyboard())
                    return
                if t in ('导出', 'export'):
                    cur = settings_service.get_listen_sources(acc_id) or []
                    listing = '\n'.join(cur) or '（空）'
                    await event.respond(f"📡 监听群组共 {len(cur)} 条：\n{listing}")
                    return
                if t in ('导入', 'import'):
                    set_state(chat_id, 'listen_sources_import_wait_file', account_id=acc_id)
                    await event.respond('📄 请发送包含群组的文本文件（每行一个，支持 @username / chat_id / t.me 链接），作为文档上传。')
                    return
                if t in ('清空',):
                    settings_service.clear_listen_sources(acc_id)
                elif t.startswith('q') or t.startswith('Q'):
                    value = t[1:].strip()
                    if value:
                        settings_service.delete_listen_source(acc_id, value)
                else:
                    # 支持多行批量新增
                    sources = [l.strip() for l in t.splitlines() if l.strip()]
                    settings_service.bulk_add_listen_sources(acc_id, sources)
                cur = settings_service.get_listen_sources(acc_id) or []
                preview = '\n'.join(['• ' + x for x in cur[:20]]) or '（空）'
                await event.respond(
                    f"📡 监听群组（共 {len(cur)} 条，预览前20条）：\n{preview}\n\n"
                    '新增：直接发送（可多行）; 删除：q值；导入/导出/清空/完成'
                )
                return

        if is_cmd(text, '设置监听群组'):
            rows = list_accounts('listen')
            if not rows:
                await event.respond('⚠️ 尚无监听账号，请先添加。')
                return
            
            # 显示所有监听账号及其监听的群组
            lines = []
            for r in rows:
                acc_id = r['id']
                ident = r['username'] or r['phone'] or f"#{acc_id}"
                sources = settings_service.get_listen_sources(acc_id) or []
                if sources:
                    sources_preview = ', '.join(sources[:3])
                    if len(sources) > 3:
                        sources_preview += f' ... (共 {len(sources)} 个)'
                    lines.append(f"• {ident} (#{acc_id}): {sources_preview}")
                else:
                    lines.append(f"• {ident} (#{acc_id}): （未设置，将监听所有群组）")
            
            summary = '\n'.join(lines) if lines else '（无监听账号）'
            
            acc_hint = extract_account_id(text)
            target_row = None
            if acc_hint:
                target_row = dao_accounts.get(acc_hint)
                if target_row and not role_allows_listen(get_account_role(acc_hint)):
                    target_row = None
            if not target_row and len(rows) == 1:
                target_row = rows[0]
            if target_row:
                acc_id = target_row['id']
                set_state(chat_id, 'listen_sources_manage', account_id=acc_id)
                cur = settings_service.get_listen_sources(acc_id) or []
                preview = '\n'.join(['• ' + x for x in cur[:20]]) or '（空）'
                await event.respond(
                    f"📡 监听群组配置\n\n"
                    f"所有监听账号的群组列表：\n{summary}\n\n"
                    f"────────────\n"
                    f"当前编辑账号 #{acc_id} 的监听群组（共 {len(cur)} 条，预览前20条）：\n{preview}\n\n"
                    '操作说明：\n'
                    '新增：直接发送 @group / 123456789 / https://t.me/xxx（支持多行）\n'
                    '删除：发送 q值（例：q@group / q123456）\n'
                    '导入：发送"导入"，上传文本文件（每行一个）\n'
                    '导出：发送"导出"\n'
                    '清空：发送"清空"\n'
                    '返回：发送"完成"'
                )
            else:
                set_state(chat_id, 'set_listen_sources_choose_account')
                await event.respond(
                    f'📡 监听群组配置\n\n'
                    f'所有监听账号的群组列表：\n{summary}\n\n'
                    f'────────────\n'
                    f'🔢 请输入要设置监听群组的账号ID：'
                )
            return

        if is_cmd(text, '设置转发目标'):
            rows = list_accounts('listen')
            if not rows:
                await event.respond('⚠️ 尚无监听账号，请先添加。')
                return
            
            # 显示所有监听账号的转发目标（只显示账号专属的）
            lines = []
            for r in rows:
                acc_id = r['id']
                ident = r['username'] or r['phone'] or f"#{acc_id}"
                account_target = settings_service.get_account_target_chat(acc_id)
                if account_target:
                    lines.append(f"• {ident} (#{acc_id}): {account_target}")
                else:
                    lines.append(f"• {ident} (#{acc_id}): （未设置）")
            
            summary = '\n'.join(lines)
            
            acc_hint = extract_account_id(text)
            target_row = None
            if acc_hint:
                target_row = dao_accounts.get(acc_hint)
                if target_row and not role_allows_listen(get_account_role(acc_hint)):
                    target_row = None
            if not target_row and len(rows) == 1:
                target_row = rows[0]
            if target_row:
                acc_id = target_row['id']
                cur = settings_service.get_account_target_chat(acc_id) or '（未设置）'
                set_state(chat_id, 'set_forward_target', account_id=acc_id)
                await event.respond(
                    f'📤 设置转发目标\n\n'
                    f'当前所有转发目标：\n{summary}\n\n'
                    f'────────────\n'
                    f'当前编辑账号 #{acc_id} 的转发目标：{cur}\n\n'
                    '请输入转发目标：\n'
                    '• 用户名：@username\n'
                    '• 群组/频道：@groupname 或 chat_id\n'
                    '• 链接：https://t.me/username\n'
                    '• 输入"清空"清除设置\n'
                    '• 输入"取消"退出'
                )
            else:
                set_state(chat_id, 'set_forward_target_choose_account')
                await event.respond(
                    f'📤 设置转发目标\n\n'
                    f'当前所有转发目标：\n{summary}\n\n'
                    f'────────────\n'
                    f'🔢 请输入要设置转发目标的账号ID：'
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
                return '\n'.join([f"• #{r['id']} {r['username'] or r['phone'] or ''} ({r['status']})" for r in items])
            summary = (
                f"📒 账号列表（共 {len(rows)} 个）\n\n"
                f"监听账号（{len(listen_rows)}）：\n{format_rows(listen_rows)}\n\n"
                f"点击账号（{len(click_rows)}）：\n{format_rows(click_rows)}"
            )
            await event.respond(summary)
            return

        if is_cmd(text, '▶️ 开始点击'):
            # 提示用户发送目标消息链接
            set_state(chat_id, 'start_click_wait_link')
            await event.respond(
                '🚀 **开始点击**\n\n'
                '请发送要点击的消息链接（支持 https://t.me/c/xxx/123 或 https://t.me/username/123 格式）。\n\n'
                '发送“取消”可退出。',
                parse_mode='markdown',
                buttons=None
            )
            return

        if is_cmd(text, '移除所有账号'):
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
            return

        if is_cmd(text, '添加监听账号'):
            set_state(chat_id, 'add_listen_account_wait_string')
            await event.respond(
                '🔑 添加监听账号（可连续）\n'
                '• 发送 StringSession 文本 或 .session 文件（作为文档）进行添加\n'
                '• 发送“完成”结束添加\n'
                '提示：StringSession 通常以 1A 开头'
            )
            return

        if is_cmd(text, '添加点击账号'):
            set_state(chat_id, 'add_click_account_wait_file')
            await event.respond(
                '🖱️ 添加点击账号（可连续）\n'
                '• 发送 .session 文件（作为文档）或 StringSession 文本进行添加\n'
                '• 发送“完成”结束添加\n'
                '提示：StringSession 通常以 1A 开头'
            )
            return

        # 批量添加入口已移除

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
            'listen_sources_import_wait_file',
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
                # auto-assign role based on entry
                if st['mode'] == 'add_click_account_wait_file':
                    settings_service.set_account_role(info['id'], 'click')
                    # stay in continuous add mode
                    await event.respond(
                        f"✅ 点击账号添加成功！\n用户昵称：{info.get('nickname') or ''}\n用户名：{info.get('username') or '无'}\n账号：{info.get('phone') or ''}\n\n继续添加：再发送文件或 StringSession 文本\n结束：发送“完成”"
                    )
                elif st['mode'] == 'add_listen_account_wait_string':
                    settings_service.set_account_role(info['id'], 'listen')
                    # stay in continuous add mode
                    await event.respond(
                        f"✅ 监听账号添加成功！\n用户昵称：{info.get('nickname') or ''}\n用户名：{info.get('username') or '无'}\n账号：{info.get('phone') or ''}\n\n继续添加：再发送文件或 StringSession 文本\n结束：发送“完成”\n（提醒目标可稍后在菜单中为该账号设置）"
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
            elif st['mode'] == 'listen_sources_import_wait_file':
                # 从文本文件批量导入监听群组
                account_id = st['pending']['account_id']

                tmp_dir = 'tmp_import'
                os.makedirs(tmp_dir, exist_ok=True)
                tmp_path = os.path.join(tmp_dir, f'src_{event.id}_{name}')
                await event.download_media(file=tmp_path)

                sources = []
                try:
                    with open(tmp_path, 'r', encoding='utf-8') as f:
                        for line in f:
                            t = (line or '').strip()
                            if t:
                                sources.append(t)
                finally:
                    try:
                        os.remove(tmp_path)
                    except OSError:
                        pass

                before = settings_service.get_listen_sources(account_id) or []
                settings_service.bulk_add_listen_sources(account_id, sources)
                after = settings_service.get_listen_sources(account_id) or []
                added = max(0, len(after) - len(before))

                set_state(chat_id, 'listen_sources_manage', account_id=account_id)
                preview = '\n'.join(['• ' + x for x in after[:20]]) or '（空）'
                await event.respond(
                    f"📥 监听群组导入完成\n"
                    f"本次新增：{added} 条，当前总数：{len(after)} 条（预览前20条）：\n{preview}\n\n"
                    "新增：直接发送（可多行）; 删除：q值；导入/导出/清空/完成"
                )
        except Exception as e:
            set_state(chat_id)
            await event.respond(f"文件处理失败：{e}", buttons=main_keyboard())

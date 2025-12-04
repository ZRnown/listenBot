import asyncio
import os
import re
import unicodedata
from typing import List, Optional
from telethon import events, TelegramClient
from telethon.tl.custom import Button
from bot.keyboards import main_keyboard, roles_keyboard
from bot.utils import set_state, get_state, is_cmd, extract_account_id, split_keywords_payload
from bot.account_utils import (
    get_account_role, role_allows_listen, role_allows_click, format_role_label,
    account_summary_text, account_base_buttons, account_menu_buttons, account_menu_text, list_accounts
)
from services import settings_service
from services import joining
from storage import dao_accounts
from storage import dao_keywords
from services import sessions as sess_service
from core.clients import ClientManager
from bot.click_tasks import parse_and_execute_click
from core.filters import normalize_text_for_matching


async def parse_and_execute_click(manager: ClientManager, link_text: str, report_chat_id: int):
    """兼容旧调用入口，实际实现已迁移至 bot.click_tasks.parse_and_execute_click。"""
    from bot.click_tasks import parse_and_execute_click as _impl
    return await _impl(manager, link_text, report_chat_id)


# start_click_job 已迁移至 bot/click_tasks.py


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
        # 监听功能已删除，统一展示为“点击”
        return '点击'

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
        # 仅支持点击关键词（监听关键词已删除）
        role_filter = 'click'
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
        dao_keywords.set_keywords(acc_id, [], kind='click')
        settings_service.clear_account_settings(acc_id)
        dao_accounts.delete(acc_id)

    @bot.on(events.NewMessage(pattern='/start'))
    async def _(event):
        # 如果用户正在某个状态中（如设置消息、设置延迟等），不处理 /start 命令
        # 让状态处理器来处理用户的输入
        chat_id = event.chat_id
        st = get_state(chat_id)
        if st:
            # 有状态时不处理 /start，让状态处理器处理
            return
        
        await event.respond(
            '🙌 欢迎使用控制面板\n\n'
            '功能一览：\n'
            '• 🧩 点击关键词管理\n'
            '• ➕ 添加点击账号（支持 StringSession 文本 或 .session 文件）\n'
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

    @bot.on(events.CallbackQuery(pattern=b'auto_join:click'))
    async def _(event):
        rows = list_accounts('click')
        active_ids = [r['id'] for r in rows if r['id'] in manager.account_clients]
        if not active_ids:
            await event.answer('暂无对应激活账号', alert=True)
            return
        set_state(event.chat_id, 'auto_join_wait_link', account_ids=active_ids, role='click')
        text = (
            "🚪 使用点击账号自动进群\n"
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
            
            if action == 'click':
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
        st = get_state(chat_id)

        # 如果用户有状态，/start 应该被当作普通文本处理，而不是命令
        # 只有在没有状态时，/start 才作为命令处理
        if not st and (text == '/start' or text.startswith('/start ')):
            return

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
        
        # 主菜单按钮文本（监听相关入口已移除）
        MAIN_MENU_COMMANDS = {
            '🧩 点击关键词',
            '📒 账号列表',
            '➕ 添加点击账号',
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
            if is_main_menu_cmd and mode in ('add_click_account_wait_file',):
                set_state(chat_id, None)
                st = None
                # 继续执行，让命令处理器处理（不在这里 return）
            else:
                # 正常处理状态
                if mode == 'bulk_keywords_input':
                    kind = st['pending']['kind']
                t = (text or '').strip()
                    rows = list_accounts('click')
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
                return
                    if t.lower() in ('清空', 'clear'):
                        if kind == 'click':
                            # 清空全局点击关键词
                            settings_service.set_global_click_keywords([])
                            # 应用到所有点击账号
                            settings_service.apply_global_click_keywords_to_all_accounts()
                set_state(chat_id)
                            await event.respond(f"🧹 已清空所有点击账号的关键字（全局设置）", buttons=main_keyboard())
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
                return

                elif mode in ('choose_account_role', 'change_account_role', 'set_account_target',
                              'set_forward_target_global', 'set_target_chat'):
                    # 以上模式全部属于监听/转发提醒相关功能，现已废弃
                    set_state(chat_id)
                    await event.respond('⚠️ 当前版本已移除监听/转发相关配置，本操作已取消。', buttons=main_keyboard())
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
                        # 如果账号已存在，保持角色；否则设置为 click
                        current_role = settings_service.get_account_role(account_id) or 'click'
                        if info.get('existing'):
                            # 账号已存在，保持角色
                            if current_role == 'click':
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
                    t = (text or '').strip().lower()
                    # 支持 "all" 或 "全部" 来应用到所有账号
                    if t in ('all', '全部', '所有'):
                        set_state(chat_id, 'set_click_delay_input', account_id='all')
                        await event.respond('⏱️ 请输入点击延迟（单位秒，可为小数，例如 0.8）\n\n（将应用到所有点击账号）')
                        return
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
                        await event.respond('⚠️ 请输入有效的账号ID（数字），或输入 "all"/"全部" 应用到所有账号')
                return

                elif mode == 'set_click_delay_input':
                account_id = st['pending']['account_id']
                    t = (text or '').strip()
                    # 支持取消操作
                    if t.lower() in ('取消', '退出', 'cancel', 'exit'):
                        set_state(chat_id)
                        await event.respond('✅ 已取消设置', buttons=main_keyboard())
                        return
                try:
                    value = float(text)
                        if account_id == 'all':
                            # 应用到所有点击账号
                            rows = list_accounts('click')
                            if not rows:
                                await event.respond('⚠️ 当前没有点击账号', buttons=main_keyboard())
                                set_state(chat_id)
                                return
                            count = 0
                            for r in rows:
                                settings_service.set_click_delay(str(value), r['id'])
                                count += 1
                            set_state(chat_id)
                            await event.respond(f'✅ 已为所有 {count} 个点击账号设置点击延迟：{value} 秒', buttons=main_keyboard())
                        else:
                    settings_service.set_click_delay(str(value), account_id)
                    set_state(chat_id)
                    await event.respond('✅ 已设置点击延迟', buttons=main_keyboard())
                except Exception:
                        await event.respond('⚠️ 请输入数字，例如 0.8，或输入"取消"退出')
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
                    import random
                    link = text
                    account_ids = st['pending'].get('account_ids', [])
                    role_sel = st['pending'].get('role', 'click')
                    if not account_ids:
                set_state(chat_id)
                        await event.respond(
                            "⚠️ 当前没有激活的点击账号，请先添加并连接成功。",
                            buttons=main_keyboard()
                        )
                return
                    # 支持取消操作
                    t = (link or '').strip()
                    if t.lower() in ('取消', '退出', 'cancel', 'exit'):
                    set_state(chat_id)
                        await event.respond('✅ 已取消进群操作', buttons=main_keyboard())
                    return
                lines = [l.strip() for l in link.splitlines() if l.strip()]
                    if not lines:
                        await event.respond('⚠️ 请发送至少一个有效的群链接或用户名，或输入"取消"退出。')
                        return
                    
                    # 发送进度提示
                    await event.respond(f'⏳ 正在自动进群，共 {len(account_ids)} 个账号…')
                    
                ok = 0
                fail = 0
                    fail_details = []
                mn, mx = settings_service.get_join_delay_range()
                    total_operations = len(lines) * len(account_ids)
                    
                for target in lines:
                        for acc_id in account_ids:
                            client = manager.account_clients.get(acc_id)
                        if not client:
                                acc_info = dao_accounts.get(acc_id)
                                acc_label = acc_info.get('username') or acc_info.get('phone') or f"#{acc_id}"
                                fail_details.append(f"账号 {acc_label}: 客户端未连接")
                                fail += 1
                            continue
                        try:
                            await joining.join_chat(client, target)
                            ok += 1
                            except Exception as e:
                                acc_info = dao_accounts.get(acc_id)
                                acc_label = acc_info.get('username') or acc_info.get('phone') or f"#{acc_id}"
                                fail_details.append(f"账号 {acc_label} -> {target}: {str(e)}")
                            fail += 1
                        await asyncio.sleep(random.uniform(mn, mx))
                    
                set_state(chat_id)
                    msg_parts = [
                        f"✅ 进群完成（共 {len(account_ids)} 个账号）",
                        f"\n📋 处理链接：{len(lines)} 个",
                        f"🐢 进群延迟：{mn:.1f}-{mx:.1f} 秒",
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

        if is_cmd(text, '设置转发目标'):
            # 监听/转发提醒功能已移除，给出提示并直接返回
            await event.respond('⚠️ 当前版本已移除"监听转发目标"功能，如需重新启用，请联系开发者修改代码。')
                        return

        # 主菜单命令处理（仅保留点击关键词）

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
                            )
                            await event.respond(result, parse_mode='markdown')
                        except Exception as e:
                            await event.respond(f'❌ 无法获取群组信息：{str(e)}\n\n可能原因：\n• 账号未加入该群组\n• 群组ID错误\n• 没有访问权限')
            return
                    except Exception as e:
                        await event.respond(f'❌ 诊断失败：{str(e)}')
                return
            else:
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
            # 监听账号功能已移除，给出提示
            await event.respond('⚠️ 当前版本已移除“监听账号”功能，请使用“➕ 添加点击账号”。')
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
            # 直接进入输入延迟值状态，默认应用到所有账号
            set_state(chat_id, 'set_click_delay_input', account_id='all')
            await event.respond(
                '⏱️ 设置点击延迟\n\n'
                '请输入点击延迟（单位秒，可为小数，例如 0.8）：\n'
                f'（将应用到所有 {len(rows)} 个点击账号）\n\n'
                '💡 输入"取消"或"退出"可取消操作'
            )
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
            try:
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
            except Exception as e:
                error_msg = f'❌ 发送过程中发生错误：{str(e)}'
                print(f"[开始发送] 错误: {e}")
                import traceback
                traceback.print_exc()
                await event.respond(error_msg, buttons=main_keyboard())
            return

        if is_cmd(text, '自动进群'):
            click_active = [r['id'] for r in list_accounts('click') if r['id'] in manager.account_clients]
            if not click_active:
                await event.respond('⚠️ 当前没有已连接的点击账号，请先确保账号在线。')
                return
            set_state(event.chat_id, 'auto_join_wait_link', account_ids=click_active, role='click')
            text = (
                "🚪 使用点击账号自动进群\n"
                "请发送群链接或 @用户名（每行一个，可多个）\n"
                "支持：https://t.me/+inviteHash / https://t.me/groupname / @groupname\n\n"
                '💡 输入"取消"或"退出"可取消操作'
            )
            await event.respond(text)
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
            'keywords_import_wait_file',
        ):
            return
        try:
            doc = event.document
            name = doc.attributes[0].file_name if doc.attributes else 'session.session'
            if st['mode'] in ('add_account_wait_file', 'add_click_account_wait_file'):
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
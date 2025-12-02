"""
账号相关工具函数
"""
from telethon.tl.custom import Button
from services import settings_service
from storage import dao_accounts


def get_account_role(account_id: int) -> str:
    """获取账号角色（监听功能已删除，统一视为点击账号）"""
    return 'click'


def role_allows_listen(role: str) -> bool:
    """监听功能已删除，任何角色都不允许监听"""
    return False


def role_allows_click(role: str) -> bool:
    """所有账号都允许作为点击账号使用"""
    return True


def format_role_label(role: str) -> str:
    """格式化角色标签（监听功能已删除，仅保留点击）"""
    return {'click': '点击'}.get(role, '点击')


def account_summary_text(row) -> str:
    """生成账号摘要文本"""
    acc_id = row['id']
    role = get_account_role(acc_id)
    ident = row['username'] or row['phone'] or ''
    status = row['status']
    start_flag = '开启' if settings_service.get_start_sending(acc_id) else '关闭'
    click_kw = len(settings_service.get_account_keywords(acc_id, kind='click') or [])
    lines = [
        f"#{acc_id} | {ident or '无用户名'} | {status}",
        f"角色：{format_role_label(role)}  ▶️ 发送：{start_flag}",
    ]
    if role_allows_click(role):
        lines.append(f"点击关键字：{click_kw} 条")
    return '\n'.join(lines)


def account_base_buttons(acc_id: int):
    """账号基础按钮"""
    return [
        [Button.inline('⚙️ 设置', data=f'acc|{acc_id}|menu'), Button.inline('🗑️ 删除', data=f'acc|{acc_id}|delete')]
    ]


def account_menu_buttons(acc_id: int):
    """账号菜单按钮"""
    role = get_account_role(acc_id)
    buttons = []
    # 监听功能已删除，只保留点击相关按钮
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
    """账号菜单文本"""
    role = get_account_role(row['id'])
    return (
        f"#{row['id']} 操作面板（{format_role_label(role)}）\n"
        "请选择要执行的操作："
    )


def list_accounts(role_filter=None):
    """列出账号（监听功能已删除，所有账号一律视为可点击账号）"""
    rows = dao_accounts.list_all()
    # 监听功能已删除：所有账号一律视为"可点击账号"
    if role_filter == 'click' or role_filter is None:
        return [r for r in rows if role_allows_click(get_account_role(r['id']))]
    # 任何关于"listen"的过滤都退化为普通账号列表
    return [r for r in rows if role_allows_click(get_account_role(r['id']))]


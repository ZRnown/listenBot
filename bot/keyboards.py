from telethon.tl.custom import Button

# 主菜单（已移除所有“监听”相关功能，只保留点击/发送/进群等功能）
MAIN_BTNS = [
    ['🧩 点击关键词'],
    ['🎯 设置目标机器人', '📝 设置发送消息', '🐢 设置发送延迟'],
    ['⏱️ 设置点击延迟', '▶️ 开始发送', '📒 账号列表'],
    ['➕ 添加点击账号'],
    ['🚪 自动进群', '🗑️ 移除所有账号'],
]

def main_keyboard():
    rows = []
    for row in MAIN_BTNS:
        rows.append([Button.text(txt) for txt in row])
    return rows


def roles_keyboard():
    """角色选择键盘（监听功能已删除，只保留点击账号）"""
    rows = []
    # 只保留“点击账号”，其他选项会导致无意义的监听配置，统一去掉
    rows.append([Button.text('点击账号')])
    rows.append([Button.text('跳过')])
    return rows

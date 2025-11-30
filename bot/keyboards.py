from telethon.tl.custom import Button

# 简化后的主菜单
MAIN_BTNS = [
    ['🧩 监听关键词', '🧩 点击关键词', '▶️ 开始点击'],
    ['🎯 设置目标机器人', '📝 设置发送消息', '🐢 设置发送延迟'],
    ['⏱️ 设置点击延迟', '▶️ 开始发送', '📒 账号列表'],
    ['📤 设置转发目标', '➕ 添加监听账号', '➕ 添加点击账号'],
    ['🚪 自动进群', '🗑️ 移除所有账号']
]

def main_keyboard():
    rows = []
    for row in MAIN_BTNS:
        rows.append([Button.text(txt) for txt in row])
    return rows


def roles_keyboard():
    rows = []
    rows.append([Button.text('监听账号'), Button.text('点击账号')])
    rows.append([Button.text('同时监听与点击')])
    rows.append([Button.text('跳过')])
    return rows

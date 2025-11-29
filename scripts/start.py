import asyncio
import os
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv
# Load env ASAP BEFORE importing modules that read env (e.g., storage.db -> app.config)
load_dotenv()

from storage.db import migrate
from core.clients import ClientManager
from bot.control_bot import setup_handlers

async def main():
    from datetime import datetime
    manager = None
    try:
        print(f"[启动日志] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 开始启动系统...")
        migrate()
        print(f"[启动日志] 数据库迁移完成")
        manager = ClientManager()
        print(f"[启动日志] 正在启动控制机器人...")
        await manager.start_control_bot()
        print(f"[启动日志] ✅ 控制机器人启动成功")
        print(f"[启动日志] 正在设置机器人处理器...")
        await setup_handlers(manager)
        print(f"[启动日志] ✅ 机器人处理器设置完成")
        print(f"[启动日志] 正在加载活跃账号...")
        await manager.load_active_accounts()
        print(f"[启动日志] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ✅ 系统启动完成，开始监听消息...")
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        # graceful shutdown on Ctrl+C
        pass
    except (GeneratorExit, RuntimeError) as e:
        # 忽略 Telethon 内部连接关闭时的错误
        if 'GeneratorExit' in str(type(e).__name__) or 'coroutine ignored' in str(e):
            return
        raise
    finally:
        if manager:
            try:
                await manager.stop()
            except (GeneratorExit, RuntimeError):
                # 忽略连接关闭时的错误
                pass
            except Exception:
                pass

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

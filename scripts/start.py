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
        print(f"[启动日志] 客户端管理器初始化完成")
        print(f"[启动日志] 正在启动控制机器人...")
    await manager.start_control_bot()
        print(f"[启动日志] ✅ 控制机器人启动成功")
        print(f"[启动日志] 正在设置机器人处理器...")
    await setup_handlers(manager)
        print(f"[启动日志] ✅ 机器人处理器设置完成")
        print(f"[启动日志] 正在加载活跃账号...")
    await manager.load_active_accounts()
        print(f"[启动日志] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ✅ 系统启动完成")
        
        # 主循环：保持程序运行（使用更可靠的方式）
        print(f"[启动日志] 程序正在运行，按 Ctrl+C 退出...")
        try:
            # 创建一个永远不会被设置的 Event，保持程序运行
            stop_event = asyncio.Event()
            await stop_event.wait()
        except asyncio.CancelledError:
            # 正常处理取消
            print(f"[启动日志] 主循环被取消")
            raise
    except KeyboardInterrupt:
        # graceful shutdown on Ctrl+C
        print(f"\n[启动日志] 收到退出信号，正在关闭...")
    except (GeneratorExit, RuntimeError) as e:
        # 忽略 Telethon 内部连接关闭时的错误
        if 'GeneratorExit' in str(type(e).__name__) or 'coroutine ignored' in str(e):
            return
        print(f"[启动日志] ❌ 运行时错误: {e}")
        import traceback
        traceback.print_exc()
    except Exception as e:
        print(f"[启动日志] ❌ 系统启动失败: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if manager:
            try:
                print(f"[启动日志] 正在关闭所有连接...")
                await manager.stop()
                print(f"[启动日志] ✅ 所有连接已关闭")
            except (GeneratorExit, RuntimeError):
                # 忽略连接关闭时的错误
                pass
            except Exception as e:
                print(f"[启动日志] ⚠️ 关闭连接时出错: {e}")

if __name__ == '__main__':
    try:
        # 使用 asyncio.run 运行主函数
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[启动日志] 程序被用户中断")
    except asyncio.CancelledError:
        print("\n[启动日志] 程序被取消")
    except Exception as e:
        print(f"[启动日志] ❌ 程序异常退出: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

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
    manager = None
    try:
        migrate()
        manager = ClientManager()
        await manager.start_control_bot()
        await setup_handlers(manager)
        await manager.load_active_accounts()
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        # graceful shutdown on Ctrl+C
        pass
    finally:
        if manager:
            try:
                await manager.stop()
            except Exception:
                pass

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

import asyncio

from muswarmlogger.main import run


loop = asyncio.get_event_loop()
try:
    loop.run_until_complete(run())
except (SystemExit, KeyboardInterrupt):
    exit(0)
finally:
    loop.run_until_complete(loop.shutdown_asyncgens())
    loop.close()

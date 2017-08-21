import asyncio

from muswarmlogger.main import run


loop = asyncio.get_event_loop()
task = loop.create_task(run())
try:
    loop.run_until_complete(task)
except (SystemExit, KeyboardInterrupt):
    exit(0)
finally:
    if not task.done():
        task.cancel()
        try:
            loop.run_until_complete(task)
        except asyncio.CancelledError:
            pass
    loop.close()

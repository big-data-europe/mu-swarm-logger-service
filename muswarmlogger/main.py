import asyncio
import importlib
import logging
from aiodockerpy import APIClient
from docker.utils.utils import kwargs_from_env
from os import environ as ENV

from muswarmlogger.events import (
    Event, cleanup_fixtures, run_on_startup_coroutines, send_event,
    startup_fixtures)


logger = logging.getLogger(__name__)


if ENV.get("ENV", "prod").startswith("dev"):
    logging.basicConfig(level=logging.DEBUG)
else:
    logging.basicConfig(level=logging.INFO)


# NOTE: find all the "loggers" modules and load them, this will trigger the
#       event registrations and on_startup coroutines registration
for name in ENV.get('LOGGERS', "sparql").split():
    importlib.import_module("muswarmlogger.loggers.%s" % name)


async def run():
    """
    Start the main loop of the application: loop on the Docker events until
    there is no more events (the daemon is dead)
    """
    docker_args = kwargs_from_env()
    docker = APIClient(**docker_args)
    try:
        parameters = await startup_fixtures(docker)
        await run_on_startup_coroutines(parameters)
        async for x in docker.events(decode=True):
            event = Event.new(docker, x)
            asyncio.ensure_future(send_event(event, parameters))
    except asyncio.CancelledError:
        for task in asyncio.Task.all_tasks():
            if task is not asyncio.Task.current_task() and not task.done():
                task.cancel()
                try:
                    await task
                except Exception:
                    if not task.cancelled():
                        logger.exception("An error occurred")
        raise
    finally:
        await cleanup_fixtures()
        await docker.close()

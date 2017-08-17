import asyncio
import importlib
import logging
from aiodockerpy import APIClient
from aiosparql.client import SPARQLClient
from aiosparql.syntax import IRI
from docker.utils.utils import kwargs_from_env
from os import environ as ENV

from muswarmlogger.events import (
    list_handlers, new_event, run_on_startup_subroutines)


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
    docker = APIClient(timeout=5, **docker_args)
    sparql = SPARQLClient(ENV['MU_SPARQL_ENDPOINT'],
                          graph=IRI(ENV['MU_APPLICATION_GRAPH']))
    try:
        await run_on_startup_subroutines(docker, sparql)
        async for x in docker.events(decode=True):
            try:
                event = new_event(docker, x)
                await asyncio.gather(
                    *(handler(event, sparql)
                    for handler in list_handlers(
                        event,
                        reload=ENV.get("ENV", "prod").startswith("dev"))))
            except Exception:
                logger.exception(
                    "An error occurred during a coroutine execution. "
                    "The loop will not be interrupted.")
    finally:
        await docker.close()
        await sparql.close()

import asyncio
import logging
from aiodockerpy import APIClient
from aiosparql.client import SPARQLClient
from aiosparql.syntax import IRI
from docker.utils.utils import kwargs_from_env
from os import environ as ENV

from muswarmlogger.events import (
    list_handlers, new_event, run_on_startup_subroutines)


logger = logging.getLogger(__name__)


async def run_loop(sparql_endpoint=None, debug=False):
    docker_args = kwargs_from_env()
    docker = APIClient(timeout=5, **docker_args)
    sparql = SPARQLClient(sparql_endpoint,
                          graph=IRI(ENV['MU_APPLICATION_GRAPH']))
    try:
        await run_on_startup_subroutines(docker, sparql)
        async for x in docker.events(decode=True):
            try:
                event = new_event(docker, x)
                await asyncio.gather(
                    *(handler(event, sparql)
                    for handler in list_handlers(event, reload=debug)))
            except Exception:
                logger.exception(
                    "An error occurred during a coroutine execution. "
                    "The loop will not be interrupted.")
    finally:
        await docker.close()
        await sparql.close()

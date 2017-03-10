import argparse
import asyncio
import logging

from muswarmlogger.aiodocker import APIClient
from muswarmlogger.events import (
    list_handlers, new_event, run_on_startup_subroutines)
from muswarmlogger.sparql import SPARQLClient, prefixes


logger = logging.getLogger()


async def run_loop(sparql_endpoint=None, debug=False):
    sparql_context = SPARQLClient(sparql_endpoint, prefixes=prefixes)
    docker_context = APIClient(timeout=5)
    with sparql_context as sparql, docker_context as docker:
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug",
        action="store_true", help="Debug mode (reload modules automatically")
    parser.add_argument("--sparql-endpoint",
        type=str, help="SPARQL endpoint (MU_SPARQL_ENDPOINT by default)")
    opt = parser.parse_args()
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(run_loop(
            sparql_endpoint=opt.sparql_endpoint, debug=opt.debug))
    finally:
        loop.close()

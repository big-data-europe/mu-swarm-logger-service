import argparse
import asyncio
from enforce import runtime_validation

from muswarmlogger.aiodocker import APIClient
from muswarmlogger.events import list_handlers, new_event, run_on_startup_subroutines
from muswarmlogger.sparql import SPARQLClient, prefixes

async def run_loop(loop=None, sparql_endpoint=None, debug=False):
    with SPARQLClient(sparql_endpoint, prefixes=prefixes, loop=loop) as sparql_client, \
            APIClient(loop=loop, timeout=5) as docker_client:
        await run_on_startup_subroutines(docker_client, sparql_client)
        async for x in docker_client.events(decode=True):
            event = new_event(docker_client, x)
            await asyncio.gather(
                *(handler(event, sparql_client)
                for handler in list_handlers(event, reload=debug)))

@runtime_validation(group='test')
def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug",
        action="store_true", help="Debug mode (reload modules automatically")
    parser.add_argument("--sparql-endpoint",
        type=str, help="SPARQL endpoint (MU_SPARQL_ENDPOINT by default)")
    opt = parser.parse_args()
    loop = asyncio.get_event_loop()
    try:
        try:
            loop.run_until_complete(run_loop(loop,
                sparql_endpoint=opt.sparql_endpoint, debug=opt.debug))
        except Exception as exc:
            raise
            print("Exception occurred:", exc)
    finally:
        loop.close()

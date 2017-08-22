import logging
from aiosparql.client import SPARQLClient
from aiosparql.syntax import IRI
from os import environ as ENV

from muswarmlogger.events import fixture

from . import events
from . import logs
from . import stats


logger = logging.getLogger(__name__)


@fixture
async def get_sparql_client():
    sparql = SPARQLClient(ENV['MU_SPARQL_ENDPOINT'],
                          graph=IRI(ENV['MU_APPLICATION_GRAPH']))
    yield sparql
    await sparql.close()

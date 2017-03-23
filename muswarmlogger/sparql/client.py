import aiohttp
import logging
from os import environ as ENV
from textwrap import dedent
from typing import Optional

from .prefix import Prefix

__all__ = ['SPARQLClient', 'SPARQLRequestFailed']


logger = logging.getLogger(__name__)


class SPARQLRequestFailed(aiohttp.http.HttpProcessingError):
    def __init__(self, code=None, message=None, explanation=None):
        super(SPARQLRequestFailed, self).__init__(
            code=code, message=message)
        self.explanation = explanation

    def __str__(self):
        base_message = super(SPARQLRequestFailed, self).__str__()
        return "%s\nExplanation:\n%s" % (base_message, self.explanation)


class SPARQLClient(aiohttp.ClientSession):
    def __init__(self,
            endpoint: Optional[str] = None,
            update_endpoint: Optional[str] =None,
            prefixes=None, loop=None):
        super(SPARQLClient, self).__init__(loop=loop)
        self.endpoint = endpoint or ENV['MU_SPARQL_ENDPOINT']
        self.update_endpoint = update_endpoint or self.endpoint
        self.prefixes = prefixes

    def _make_query(self, query: str) -> str:
        if self.prefixes == None:
            return query
        else:
            lines = [
                "PREFIX %s: <%s>" % (x.label, x.base_uri)
                for x in vars(self.prefixes).values() if isinstance(x, Prefix)
            ]
            lines.extend(["", dedent(query).strip()])
            return "\n".join(lines)

    async def query(self, query: str, options: dict = {}) -> dict:
        headers = {
            "Accept": "application/json",
        }
        full_query = self._make_query(query)
        logger.debug("Sending SPARQL query to %s: %r",
                     self.endpoint, full_query)
        async with self.post(self.endpoint, data={"query": full_query},
                             headers=headers) as resp:
            try:
                resp.raise_for_status()
            except aiohttp.HttpProcessingError as exc:
                explanation = await resp.text()
                raise SPARQLRequestFailed(
                    code=exc.code, message=exc.message, explanation=explanation)
            return await resp.json()

    async def update(self, query: str, options: dict = {}) -> dict:
        headers = {
            "Accept": "application/json",
        }
        full_query = self._make_query(query)
        logger.debug("Sending SPARQL query to %s: %r",
                     self.endpoint, full_query)
        async with self.post(self.update_endpoint, data={"update": full_query},
                             headers=headers) as resp:
            try:
                resp.raise_for_status()
            except aiohttp.HttpProcessingError as exc:
                explanation = await resp.text()
                raise SPARQLRequestFailed(
                    code=exc.code, message=exc.message, explanation=explanation)
            return await resp.json()

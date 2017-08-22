import asyncio
import logging
from aiodockerpy import APIClient
from aiosparql.client import SPARQLClient
from aiosparql.escape import escape_string
from aiosparql.syntax import IRI, Node, Triples
from datetime import datetime
from dateutil import parser as datetime_parser
from uuid import uuid1

from muswarmlogger.events import ContainerEvent, register_event, on_startup

from .prefixes import Dct, Mu, SwarmUI


logger = logging.getLogger(__name__)


@on_startup
async def start_logging_existing_containers(docker: APIClient, sparql: SPARQLClient):
    """
    Start logging the existing container's logs to the database on startup
    """
    now = datetime.utcnow()
    containers = await docker.containers()
    for container in containers:
        container = await docker.inspect_container(container['Id'])
        if not container['Config']['Labels'].get('LOG'):
            continue
        if container['Config']['Tty']:
            continue
        container_concept = await create_container_log_concept(sparql, container)
        asyncio.ensure_future(save_container_logs(docker, container['Id'], now, sparql, container_concept))
        logger.info("Logging container %s into %s", container['Id'][:12], container_concept)


@register_event
async def start_logging_container(event: ContainerEvent, sparql: SPARQLClient):
    """
    Start logging the container's logs to the database
    """
    if not event.status == "start":
        return
    if not event.attributes.get('LOG'):
        return
    container = await event.container
    if container['Config']['Tty']:
        return
    container_concept = await create_container_log_concept(sparql, container)
    asyncio.ensure_future(save_container_logs(event.client, event.id, event.time, sparql, container_concept))
    logger.info("Logging container %s into %s", container['Id'][:12], container_concept)


async def create_container_log_concept(sparql, container):
    """
    Create a container log concept, this is the node that will group all the
    log lines together
    """
    concept = IRI("docklogs:%s" % container['Id'])
    resp = await sparql.query("ASK FROM {{graph}} WHERE { {{}} ?p ?o }",
                              concept)
    if not resp['boolean']:
        resp = await sparql.update(
            "WITH {{graph}} INSERT DATA { {{}} dct:title {{}} }",
            concept, escape_string(container['Name'][1:]))
        logger.info("Created logging concept: %s", concept)
    return concept


async def save_container_logs(client, container, since, sparql, base_concept):
    """
    Iterates over the container's log lines and insert triples to the database
    until there is no more lines
    """
    try:
        async for line in client.logs(container, stream=True, timestamps=True,
                                      since=since):
            timestamp, log = line.decode().split(" ", 1)
            timestamp = datetime_parser.parse(timestamp)
            uuid = uuid1(0)
            concept = base_concept + ("/log/%s" % uuid)
            logger.debug("Log into %s: %s", concept, log.strip())
            triples = Triples([
                (base_concept, SwarmUI.logLine, concept),
                Node(concept, {
                    Mu.uuid: uuid,
                    Dct.issued: timestamp,
                    Dct.title: log,
                }),
            ])
            resp = await sparql.update(
                """
                WITH {{graph}}
                INSERT DATA {
                    {{}}
                }
                """, triples)
    finally:
        logger.info("Finished logging into %s (container %s is stopped)",
                    base_concept, container[:12])

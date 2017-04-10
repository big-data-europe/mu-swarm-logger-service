from aiodockerpy import APIClient
import asyncio
from datetime import datetime
import logging
from os import environ as ENV
from uuid import uuid1

from muswarmlogger.events import ContainerEvent, register_event, on_startup
from muswarmlogger.sparql import SPARQLClient, escape_string
from muswarmlogger.event2rdf import Event2RDF


logger = logging.getLogger(__name__)
graph = ENV['MU_APPLICATION_GRAPH']


async def create_container_log_concept(sparql, base_concept, container):
    concept = "%s/container/%s" % (base_concept, container['Id'][:12])
    resp = await sparql.query("""
        ASK
        FROM <%(graph)s>
        WHERE {
            <%(concept)s> ?p ?o .
        }
        """ % {
            "graph": graph,
            "concept": concept,
        })
    if not resp['boolean']:
        resp = await sparql.update("""
            WITH <%(graph)s>
            INSERT DATA {
                <%(concept)s> dct:title %(name)s .
            }
            """ % {
                "graph": graph,
                "concept": concept,
                "name": escape_string(container['Name'][1:]),
            })
        logger.info("Created logging concept: %s", concept)
    return concept


async def save_container_logs(client, container, since, sparql, base_concept):
    logs = await client.logs(container, stream=True, timestamps=True, since=since)
    async for line in logs:
        timestamp, log = line.decode().split(" ", 1)
        uuid = uuid1(0)
        concept = "%s/log/%s" % (base_concept, uuid)
        logger.debug("Log into %s: %s", concept, log.strip())
        resp = await sparql.update("""
            WITH <%(graph)s>
            INSERT DATA {
                <%(base_concept)s> swarmui:logLine <%(concept)s> .

                <%(concept)s> mu:uuid %(uuid)s ;
                dct:issued %(timestamp)s^^xsd:dateTime ;
                dct:title %(log)s .
            }
            """ % {
                "graph": graph,
                "base_concept": base_concept,
                "concept": concept,
                "uuid": escape_string(str(uuid)),
                "timestamp": escape_string(timestamp),
                "log": escape_string(log),
            })
    logger.info("Finished logging into %s (container %s is stopped)",
                base_concept, container[:12])


@register_event
async def store_events(event: ContainerEvent, sparql: SPARQLClient):
    e2rdf = Event2RDF()
    e2rdf.add_event_to_graph(event.data)
    ntriples = e2rdf.serialize().decode("utf-8")
    await sparql.update("""
        WITH <%(graph)s>
        INSERT DATA {
            %(ntriples)s
        }
        """ % {
            "graph": graph,
            "ntriples": ntriples,
        })


@register_event
async def start_logging_container(event: ContainerEvent, sparql: SPARQLClient):
    if not event.status == "start":
        return
    concept = event.attributes.get('muLoggingConcept')
    if not concept:
        return
    container = await event.container
    if container['Config']['Tty']:
        return
    container_concept = await create_container_log_concept(sparql, concept, container)
    asyncio.ensure_future(save_container_logs(event.client, event.id, event.time, sparql, container_concept))
    logger.info("Logging container %s into %s", container['Id'][:12], container_concept)


@on_startup
async def start_logging_existing_containers(docker: APIClient, sparql: SPARQLClient):
    now = datetime.utcnow()
    containers = await docker.containers()
    for container in containers:
        container = await docker.inspect_container(container['Id'])
        concept = container['Config']['Labels'].get('muLoggingConcept')
        if not concept:
            continue
        if container['Config']['Tty']:
            continue
        container_concept = await create_container_log_concept(sparql, concept, container)
        asyncio.ensure_future(save_container_logs(docker, container['Id'], now, sparql, container_concept))
        logger.info("Logging container %s into %s", container['Id'][:12], container_concept)

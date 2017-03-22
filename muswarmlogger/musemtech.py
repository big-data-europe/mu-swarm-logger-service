from aiodockerpy import APIClient
import asyncio
from datetime import datetime
from uuid import uuid1

from muswarmlogger.events import ContainerEvent, register_event, on_startup
from muswarmlogger.sparql import SPARQLClient, escape_string


async def create_container_log_concept(sparql, base_concept, container):
    concept = "%s/container/%s" % (base_concept, container['Id'][:12])
    resp = await sparql.query("""
        ASK
        FROM <%(graph)s>
        WHERE {
            <%(concept)s> ?p ?o .
        }
        """ % {
            "graph": "http://mu.semte.ch/application",
            "concept": concept,
        })
    if not resp['boolean']:
        resp = await sparql.update("""
            WITH <%(graph)s>
            INSERT DATA {
                <%(concept)s> dct:title %(name)s .
            }
            """ % {
                "graph": "http://mu.semte.ch/application",
                "concept": concept,
                "name": escape_string(container['Name'][1:]),
            })
        print("-- Created concept:", concept)
    return concept


async def save_container_logs(client, container, since, sparql, base_concept):
    logs = await client.logs(container, stream=True, timestamps=True, since=since)
    async for line in logs:
        timestamp, log = line.decode().split(" ", 1)
        uuid = uuid1(0)
        concept = "%s/log/%s" % (base_concept, uuid)
        resp = await sparql.update("""
            WITH <%(graph)s>
            INSERT DATA {
                <%(base_concept)s> swarmui:logLine <%(concept)s> .

                <%(concept)s> mu:uuid %(uuid)s ;
                dct:issued %(timestamp)s^^xsd:dateTime ;
                dct:title %(log)s .
            }
            """ % {
                "graph": "http://mu.semte.ch/application",
                "base_concept": base_concept,
                "concept": concept,
                "uuid": escape_string(str(uuid)),
                "timestamp": escape_string(timestamp),
                "log": escape_string(log),
            })
        print("-- Log", concept, "--", log.strip())


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
    print("-- Started logging to", container_concept)


@on_startup
async def start_logging_existing_containers(docker: APIClient, sparql: SPARQLClient):
    now = datetime.utcnow()
    containers = await docker.containers()
    for container in containers:
        container = await docker.inspect_container(container['Id'])
        concept = container['Config']['Labels'].get('muLoggingConcept')
        if not concept:
            return
        if container['Config']['Tty']:
            return
        container_concept = await create_container_log_concept(sparql, concept, container)
        asyncio.ensure_future(save_container_logs(docker, container['Id'], now, sparql, container_concept))
        print("-- Started logging to", container_concept)

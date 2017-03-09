import asyncio
from datetime import datetime
from uuid import uuid1

from dockersyncgraph.events import ContainerEvent, register_event, on_startup
from dockersyncgraph.sparql import SPARQLClient, escape_string


async def create_container_log_concept(client, base_concept, container):
    concept = "%s/container/%s" % (base_concept, container['Id'][:12])
    resp = await client.query("""
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
        resp = await client.update("""
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

async def save_container_logs(client, container, since, sparql_client, base_concept):
    logs = await client.logs(container, stream=True, timestamps=True, since=since)
    async for line in logs:
        timestamp, log = line.decode().split(" ", 1)
        uuid = uuid1(0)
        concept = "%s/log/%s" % (base_concept, uuid)
        resp = await sparql_client.update("""
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
async def start_logging_container(event: ContainerEvent, client: SPARQLClient):
    if not event.status == "start":
        return
    concept = event.attributes.get('muLoggingConcept')
    if not concept:
        return
    container = await event.container
    if container['Config']['Tty']:
        return
    container_concept = await create_container_log_concept(client, concept, container)
    asyncio.ensure_future(save_container_logs(event.client, event.id, event.time, client, container_concept))
    print("-- Started logging to", container_concept)

@on_startup
async def start_logging_existing_containers(docker_client, sparql_client):
    now = datetime.utcnow()
    containers = await docker_client.containers()
    for container in containers:
        container = await docker_client.inspect_container(container['Id'])
        concept = container['Config']['Labels'].get('muLoggingConcept')
        if not concept:
            return
        if container['Config']['Tty']:
            return
        container_concept = await create_container_log_concept(sparql_client, concept, container)
        asyncio.ensure_future(save_container_logs(docker_client, container['Id'], now, sparql_client, container_concept))
        print("-- Started logging to", container_concept)

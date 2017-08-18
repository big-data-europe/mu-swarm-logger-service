from aiodockerpy import APIClient
import asyncio
from datetime import datetime
import logging

from muswarmlogger.events import ContainerEvent, register_event, on_startup


logger = logging.getLogger(__name__)


async def save_container_logs(client, container, since):
    """
    Print container logs to STDOUT
    """
    async for line in client.logs(container, stream=True, timestamps=True,
                                  since=since):
        timestamp, log = line.split(b" ", 1)
        print(container[:12], "--", timestamp.decode(), "--", repr(log))
    logger.info("Finished logging (container %s is stopped)", container[:12])


@register_event
async def start_logging_container(event: ContainerEvent):
    """
    Docker Container hook, if there is a LOG label, it will start saving the
    log lines to STDOUT
    """
    if not event.status == "start":
        return
    if not event.attributes.get('LOG'):
        return
    container = await event.container
    if container['Config']['Tty']:
        return
    asyncio.ensure_future(save_container_logs(event.client, event.id, event.time))
    logger.info("Logging container %s", container['Id'][:12])


@on_startup
async def start_logging_existing_containers(docker: APIClient):
    """
    On startup of the application, this function will start logging to STDOUT
    all containers that have a LOG label
    """
    now = datetime.utcnow()
    containers = await docker.containers()
    for container in containers:
        container = await docker.inspect_container(container['Id'])
        if not container['Config']['Labels'].get('LOG'):
            continue
        if container['Config']['Tty']:
            continue
        asyncio.ensure_future(save_container_logs(docker, container['Id'], now))
        logger.info("Logging container %s", container['Id'][:12])

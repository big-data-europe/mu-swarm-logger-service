from aiodockerpy import APIClient
import asyncio
from datetime import datetime
import logging
from os import environ as ENV, path

from muswarmlogger.events import ContainerEvent, register_event, on_startup


logger = logging.getLogger(__name__)
output_dir = ENV["LOG_DIR"]


async def save_container_logs(client, container, since):
    """
    Save container's log lines to a file
    """
    with open(path.join(output_dir, container), "a") as fh:
        async for line in client.logs(container, stream=True, timestamps=True,
                                      since=since):
            timestamp, log = line.decode().split(" ", 1)
            print(timestamp, "|", log.rstrip(), file=fh)
        logger.info("Finished logging (container %s is stopped)", container[:12])


@register_event
async def start_logging_container(event: ContainerEvent):
    """
    Docker Container hook, if there is a LOG label, it will start saving the
    log lines to a file
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
    On startup of the application, this function will start logging to a file
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

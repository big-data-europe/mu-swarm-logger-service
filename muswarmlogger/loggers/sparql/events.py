import logging
from aiosparql.client import SPARQLClient
from aiosparql.syntax import Node, RDF
from datetime import datetime

from muswarmlogger.events import ContainerEvent, register_event

from .prefixes import (
    DockContainer, DockContainerNetwork, DockEvent, DockEventActor,
    DockEventActions, DockEventTypes)


logger = logging.getLogger(__name__)


@register_event
async def store_events(event: ContainerEvent, sparql: SPARQLClient):
    """
    Convert a Docker container event to triples and insert them to the database
    """
    container = (await event.container) if event.status == "start" else None

    event_id = event.data.get("id", "")
    if event_id == "":
        return None

    _time = event.data.get("time", "")
    _timeNano = event.data.get("timeNano", "")
    _datetime = datetime.fromtimestamp(int(_time))

    event_id = "%s_%s" % (event_id, _timeNano)
    event_node = Node(DockEvent.__iri__+ event_id, {
        RDF.type: DockEventTypes.event,
        DockEvent.eventId: event_id,
        DockEvent.time: _time,
        DockEvent.timeNano: _timeNano,
        DockEvent.dateTime: _datetime,
    })

    event_type = event.data.get("Type", "")
    event_node.append(
        (DockEvent.type, getattr(DockEventTypes, event_type)))

    event_action = event.data.get("Action", "")
    if ":" in event_action:
        event_action_type = event_action.split(":")[0]
        event_action_extra = event_action.split(":")[-1].strip()
        event_node.append((DockEvent.actionExtra, event_action_extra))
    else:
        event_action_type = event_action

    event_node.append(
        (DockEvent.action, getattr(DockEventActions, event_action_type)))

    if container is not None:
        container_id = "%s_%s" % (container["Id"], _timeNano)
        container_node = Node(DockContainer.__iri__ + container_id, {
            DockContainer.id: container["Id"],
            DockContainer.name: container["Name"],
        })
        for label, value in container["Config"]["Labels"].items():
            container_node.append(
                (DockContainer.label, "%s=%s" % (label, value)))
        for env_with_value in container["Config"]["Env"]:
            container_node.append((DockContainer.env, env_with_value))
        event_node.append((DockEvent.container, container_node))
        for name, network in \
                container["NetworkSettings"]["Networks"].items():
            network_id = "%s_%s" % (network["NetworkID"], _timeNano)
            network_node = Node(
                DockContainerNetwork.__iri__ + network_id,
                {
                    DockContainerNetwork.name: name,
                    DockContainerNetwork.id: network["NetworkID"],
                    DockContainerNetwork.ipAddress: network["IPAddress"],
                })
            if network.get("Links"):
                for link in network["Links"]:
                    network_node.append((DockEvent.link, link))
            container_node.append((DockContainer.network, network_node))

    actor = event.data.get("Actor", "")
    if actor != "":
        actor_id = actor.get("ID", "")
        actor_id = "%s_%s" % (actor_id, _timeNano)
        actor_node = Node(DockEventActor.__iri__ + actor_id, {
            DockEventActor.actorId: actor_id,
        })
        actor_attributes = actor.get("Attributes", {})
        actor_node.extend([
            (DockEventActor.image, actor_attributes.get("image", "")),
            (DockEventActor.name, actor_attributes.get("name", "")),
            (DockEventActor.nodeIpPort, actor_attributes.get("node.addr", "")),
            (DockEventActor.nodeId, actor_attributes.get("node.id", "")),
            (DockEventActor.nodeIp, actor_attributes.get("node.ip", "")),
            (DockEventActor.nodeName, actor_attributes.get("node.name", "")),
        ])
        event_node.append((DockEvent.actor, actor_node))

    _from = event.data.get("from", "")
    if _from != "":
        event_node.append((DockEvent.source, _from))

    await sparql.update(
        """
        INSERT DATA {
            GRAPH {{graph}} {
                {{}}
            }
        }
        """, event_node)

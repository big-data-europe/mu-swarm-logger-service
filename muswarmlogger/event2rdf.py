import datetime
from aiosparql.syntax import Node, Triples

from muswarmlogger.prefixes import (
    DockContainer, DockContainerNetwork, DockEvent, DockEventActions,
    DockEventTypes)


class Event2RDF(object):
    def __init__(self):
        self.triples = Triples()

    def add_event_to_graph(self, event, container=None):
        event_id = event.get("id", "")
        if event_id == "":
            return None

        _time = event.get("time", "")
        _timeNano = event.get("timeNano", "")
        _datetime = datetime.datetime.fromtimestamp(int(_time))

        event_id = "%s_%s" % (event_id, _timeNano)
        event_node = Node("<dockevent:%s>" % event_id, {
            "a": DockEventTypes.event,
            DockEvent.eventId: event_id,
            DockEvent.time: _time,
            DockEvent.timeNano: _timeNano,
            DockEvent.dateTime: _datetime,
        })

        event_type = event.get("Type", "")
        event_node.append(
            (DockEvent.type, getattr(DockEventTypes, event_type)))

        event_action = event.get("Action", "")
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
            container_node = Node("<dockcontainer:%s>" % container_id, {
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
                    "<dockcontainer_network:%s>" % network_id,
                    {
                        DockContainerNetwork.name: name,
                        DockContainerNetwork.id: network["NetworkID"],
                        DockContainerNetwork.ipAddress: network["IPAddress"],
                    })
                if network.get("Links"):
                    for link in network["Links"]:
                        network_node.append((DockEvent.link, link))
                container_node.append((DockContainer.network, network_node))

        actor = event.get("Actor", "")
        if actor != "":
            actor_id = actor.get("ID", "")
            actor_id = "%s_%s" % (actor_id, _timeNano)
            actor_node = Node("<dockevent_actors:%s>" % actor_id, {
                DockEvent.actorId: actor_id,
            })
            actor_attributes = actor.get("Attributes", {})
            actor_node.extend([
                (DockEvent.image, actor_attributes.get("image", "")),
                (DockEvent.name, actor_attributes.get("name", "")),
                (DockEvent.nodeIpPort, actor_attributes.get("node.addr", "")),
                (DockEvent.nodeId, actor_attributes.get("node.id", "")),
                (DockEvent.nodeIp, actor_attributes.get("node.ip", "")),
                (DockEvent.nodeName, actor_attributes.get("node.name", "")),
            ])
            event_node.append((DockEvent.actor, actor_node))

        _from = event.get("from", "")
        if _from != "":
            event_node.append((DockEvent.source, _from))

        self.triples.append(event_node)

    def serialize(self):
        return str(self.triples)

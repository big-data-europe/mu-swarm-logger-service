import cached_property
import docker.errors
from enforce import runtime_validation
import importlib
import os, sys
from typing import Callable, Dict, List

from dockersyncgraph.docker import APIClient
from dockersyncgraph.sparql import SPARQLClient


class Event:
    def __init__(self, client: APIClient, data: dict):
        self.client = client
        self.data = data

    @property
    def type(self):
        return self.data['Type']

    @property
    def action(self):
        return self.data['Action']

    @property
    def id(self):
        return self.data['Actor']['ID']

    @property
    def attributes(self):
        return self.data['Actor']['Attributes']

    @property
    def time(self):
        return self.data['time']

    @property
    def time_nano(self):
        return self.data['timeNano']

on_startup_subroutines = []
event_handlers = []
module_mtimes = {}

def on_startup(subroutine: Callable[[APIClient, SPARQLClient], None]) -> None:
    on_startup_subroutines.append(subroutine)

async def run_on_startup_subroutines(docker_client: APIClient, sparql_client: SPARQLClient) -> None:
    for subroutine in on_startup_subroutines:
        await subroutine(docker_client, sparql_client)

@runtime_validation(group='test')
def register_event(subroutine: Callable[[Event, SPARQLClient], None]) -> None:
    module_name = subroutine.__module__
    module = sys.modules[module_name]
    stat_info = os.stat(module.__file__)
    if module_name not in module_mtimes:
        module_mtimes[module_name] = stat_info.st_mtime
    event_handlers.append(subroutine)

@runtime_validation(group='test')
def detect_changes(handlers: List[Callable]) -> Dict[str, float]:
    module_changes = {}
    for subroutine in handlers:
        module_name = subroutine.__module__
        module = sys.modules[module_name]
        stat_info = os.stat(module.__file__)
        if module_mtimes[module_name] != stat_info.st_mtime:
            module_changes[module_name] = stat_info.st_mtime
    return module_changes

@runtime_validation(group='test')
def reload_modules(module_changes: Dict[str, float]) -> None:
    for module_name, st_mtime in module_changes.items():
        event_handlers[:] = [
            x
            for x in event_handlers
            if x.__module__ != module_name
        ]
        importlib.reload(sys.modules[module_name])
        module_mtimes[module_name] = st_mtime

@runtime_validation(group='test')
def _filter_handlers(event: Event):
    event_type = type(event)
    return [
        event_handler
        for event_handler in event_handlers
        if event_handler.__annotations__['event'] in (event_type, Event)
    ]

@runtime_validation(group='test')
def list_handlers(event: Event, reload=False):
    handlers = _filter_handlers(event)
    if reload:
        module_changes = detect_changes(handlers)
        if module_changes:
            print("Reloading modules:", ", ".join(module_changes.keys()), file=sys.stderr)
            reload_modules(module_changes)
            handlers = _filter_handlers(event)
    return handlers

def new_event(client: APIClient, data: dict):
    event_type = data['Type']
    if event_type == "container":
        return ContainerEvent(client, data)
    else:
        import sys
        print("Not recognized event (%s):" % event_type, data, file=sys.stderr)
        return Event(client, data)

class ContainerEvent(Event):
    #@cached_property.threaded_cached_property
    @property
    def container(self):
        try:
            return self.client.inspect_container(self.id)
        except docker.errors.NotFound:
            return None

    @property
    def name(self):
        return self.attributes['name']

    @property
    def status(self):
        return self.data['status']

import asyncio
import importlib
import logging
import os, sys
from os import environ as ENV

__all__ = [
    "Event", "ContainerEvent", "register_event", "run_on_startup_coroutines",
    "send_event",
]


DEBUG = ENV.get("ENV", "prod").startswith("dev")


logger = logging.getLogger(__name__)
on_startup_coroutines = []
event_handlers = []
module_mtimes = {}


class Event:
    """
    A Docker Event
    """
    @classmethod
    def new(cls, client, data):
        """
        New Docker events are transformed here to the class Event
        """
        event_type = data['Type']
        if event_type == "container":
            return ContainerEvent(client, data)
        else:
            logger.debug("Unrecognized event (%s): %s", event_type, data)
            return Event(client, data)

    def __init__(self, client, data):
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


class ContainerEvent(Event):
    """
    A Docker container event
    """

    _container_task = None

    @property
    def container(self):
        if self._container_task is None:
            self._container_task = asyncio.ensure_future(
                self.client.inspect_container(self.id))
        return self._container_task

    @property
    def name(self):
        return self.attributes['name']

    @property
    def status(self):
        return self.data['status']


async def send_event(event, parameters):
    """
    Send the Docker event to all the registered hooks
    """
    coros_or_futures = []
    for handler in list_handlers(event):
        kwargs = get_kwargs_for_parameters(handler, [event] + parameters)
        coros_or_futures.append(handler(**kwargs))
    results = await asyncio.gather(*coros_or_futures, return_exceptions=True)
    exceptions = filter(lambda x: isinstance(x, BaseException), results)
    for exc in exceptions:
        try:
            raise exc
        except:
            logger.exception("An error occurred")


async def run_on_startup_coroutines(parameters):
    """
    This function starts all the on_startup coroutines
    """
    for coroutine in on_startup_coroutines:
        try:
            kwargs = get_kwargs_for_parameters(coroutine, parameters)
            await coroutine(**kwargs)
        except Exception:
            logger.exception("Startup coroutine failed")


def on_startup(coroutine):
    """
    A decorator that can be used to register a coroutine that is called when
    the application starts up
    """
    on_startup_coroutines.append(coroutine)


def register_event(coroutine):
    """
    A decorator that can be used to register a coroutine as a receiver of
    Docker events
    """
    module_name = coroutine.__module__
    module = sys.modules[module_name]
    stat_info = os.stat(module.__file__)
    if module_name not in module_mtimes:
        module_mtimes[module_name] = stat_info.st_mtime
    event_handlers.append(coroutine)


def get_kwargs_for_parameters(func, parameters):
    """
    Get the keyword arguments passed to a function according to the type
    annotations of that function and the list of possible parameters
    """
    return {
        key: p
        for key, type_ in func.__annotations__.items()
        for p in parameters
        if isinstance(p, type_)
    }


def list_handlers(event):
    """
    List all the handlers for a specific type of event
    """
    handlers = filter_handlers(event)
    if DEBUG:
        changes = detect_changes(handlers)
        if changes:
            logger.debug("Reloading modules: %s", ", ".join(changes.keys()))
            reload_modules(changes)
            handlers = filter_handlers(event)
    return handlers


def detect_changes(handlers):
    """
    Detect changes in the code source
    """
    changes = {}
    for coroutine in handlers:
        module_name = coroutine.__module__
        module = sys.modules[module_name]
        stat_info = os.stat(module.__file__)
        if module_mtimes[module_name] != stat_info.st_mtime:
            changes[module_name] = stat_info.st_mtime
    return changes


def reload_modules(changes):
    """
    Reload the modules that have changes
    """
    for module_name, st_mtime in changes.items():
        event_handlers[:] = [
            x
            for x in event_handlers
            if x.__module__ != module_name
        ]
        importlib.reload(sys.modules[module_name])
        module_mtimes[module_name] = st_mtime


def filter_handlers(event):
    """
    Filter the handlers for a specific type of event
    """
    event_type = type(event)
    return [
        event_handler
        for event_handler in event_handlers
        if any(issubclass(event_type, x)
               for x in event_handler.__annotations__.values())
    ]

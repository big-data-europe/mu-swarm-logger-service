import asyncio
import logging
from aiodockerpy import APIClient
from aiosparql.client import SPARQLClient
from aiosparql.syntax import Node, RDF, RDFTerm, Triples
from datetime import datetime
from dateutil import parser as datetime_parser
from elasticsearch import Elasticsearch
from os import environ as ENV
from uuid import uuid1

from muswarmlogger.events import ContainerEvent, register_event, on_startup

from .prefixes import Mu, SwarmUI


logger = logging.getLogger(__name__)


@on_startup
async def start_logging_existing_containers_stats(docker: APIClient, sparql: SPARQLClient):
    """
    Start logging the existing container's stats to the database on startup
    """
    now = datetime.utcnow()
    containers = await docker.containers()
    for container in containers:
        container = await docker.inspect_container(container['Id'])
        if not container['Config']['Labels'].get('STATS'):
            continue
        asyncio.ensure_future(save_container_stats(docker, container['Id'], now, sparql))
        logger.info("Logging container %s", container['Id'][:12])


@register_event
async def start_logging_container_stats(event: ContainerEvent, sparql: SPARQLClient):
    """
    Start logging the container stats to the database
    """
    if not event.status == "start":
        return
    if not event.attributes.get('STATS'):
        return
    container = await event.container
    asyncio.ensure_future(save_container_stats(event.client, event.id, event.time, sparql))
    logger.info("Logging container %s", container['Id'][:12])


async def save_container_stats(client, container, since, sparql):
    """
    Docker stats API doc:
    https://docs.docker.com/engine/api/v1.26/#operation/ContainerStats
    """
    async for data in client.stats(container, decode=True):
        uuid_pids_stats = uuid1(0)
        uuid_cpu_stats_cpu_usage = uuid1(0)
        uuid_cpu_stats_throttling_data = uuid1(0)
        uuid_cpu_stats = uuid1(0)
        uuid_precpu_stats_cpu_usage = uuid1(0)
        uuid_precpu_stats_throttling_data = uuid1(0)
        uuid_precpu_stats = uuid1(0)
        uuid_memory_stats_stats = uuid1(0)
        uuid_memory_stats = uuid1(0)
        uuid = uuid1(0)

        stats_node = Node(':%s' % uuid, {
            RDF.type: SwarmUI.Stats,
            Mu.uuid: uuid,
            'swarmui:read': datetime_parser.parse(data['read']),
            'swarmui:preread': datetime_parser.parse(data['preread']),
            'swarmui:pidsStats': RDFTerm(':%s' % uuid_pids_stats),
            'swarmui:numProcs': data['num_procs'],
            'swarmui:cpuStats': RDFTerm(':%s' % uuid_cpu_stats),
            'swarmui:precpuStats': RDFTerm(':%s' % uuid_precpu_stats),
            'swarmui:memoryStats': RDFTerm(':%s' % uuid_memory_stats),
            'swarmui:name': data['name'],
            'swarmui:id': data['id'],
        })

        triples = Triples([stats_node])

        for if_, network in data.get('networks', {}).items():
            network_uuid = uuid1(0)
            network_node = Node(':%s' % network_uuid, {
                RDF.type: SwarmUI.Network,
                Mu.uuid: network_uuid,
                'swarmui:interface': if_,
                'swarmui:rxBytes': network['rx_bytes'],
                'swarmui:rxPackets': network['rx_packets'],
                'swarmui:rxErrors': network['rx_errors'],
                'swarmui:rxDropped': network['rx_dropped'],
                'swarmui:txBytes': network['tx_bytes'],
                'swarmui:txPackets': network['tx_packets'],
                'swarmui:txErrors': network['tx_errors'],
                'swarmui:txDropped': network['tx_dropped'],
            })
            triples.append(network_node)
            stats_node.append(('swarmui:network', network_node))

        triples.extend([
            Node(':%s' % uuid_pids_stats, {
                RDF.type: SwarmUI.PidsStats,
                Mu.uuid: uuid_pids_stats,
                'swarmui:current': data['pids_stats']['current'],
            }),
            Node(':%s' % uuid_cpu_stats_cpu_usage, [
                (RDF.type, SwarmUI.CpuUsage),
                (Mu.uuid, uuid_cpu_stats_cpu_usage),
                ('swarmui:totalUsage', data['cpu_stats']['cpu_usage']['total_usage']),
            ] + [
                ('swarmui:percpuUsage', x)
                for x in data['cpu_stats']['cpu_usage'].get('percpu_usage', [])
            ] + [
                ('swarmui:usageInKernelmode', data['cpu_stats']['cpu_usage']['usage_in_kernelmode']),
                ('swarmui:usageInUsermode', data['cpu_stats']['cpu_usage']['usage_in_usermode']),
            ]),
            Node(':%s' % uuid_cpu_stats_throttling_data, {
                RDF.type: SwarmUI.ThrottlingData,
                Mu.uuid: uuid_cpu_stats_throttling_data,
                'swarmui:periods': data['cpu_stats']['throttling_data']['periods'],
                'swarmui:throttledPeriods': data['cpu_stats']['throttling_data']['throttled_periods'],
                'swarmui:throttledTime': data['cpu_stats']['throttling_data']['throttled_time'],
            }),
            Node(':%s' % uuid_cpu_stats, {
                RDF.type: SwarmUI.CpuStats,
                Mu.uuid: uuid_cpu_stats,
                'swarmui:cpuUsage': RDFTerm(':%s' % uuid_cpu_stats_cpu_usage),
                'swarmui:systemCpuUsage': data['cpu_stats']['system_cpu_usage'],
                'swarmui:throttlingData': RDFTerm(':%s' % uuid_cpu_stats_throttling_data),
                'swarmui:onlineCpus': data['precpu_stats'].get('online_cpus')
            }),
            Node(':%s' % uuid_precpu_stats_cpu_usage, [
                (RDF.type, SwarmUI.CpuUsage),
                (Mu.uuid, uuid_precpu_stats_cpu_usage),
                ('swarmui:totalUsage', data['precpu_stats']['cpu_usage']['total_usage']),
            ] + [
                ('swarmui:percpuUsage', x)
                for x in data['precpu_stats']['cpu_usage'].get('percpu_usage', [])
            ] + [
                ('swarmui:usageInKernelmode', data['precpu_stats']['cpu_usage']['usage_in_kernelmode']),
                ('swarmui:usageInUsermode', data['precpu_stats']['cpu_usage']['usage_in_usermode']),
            ]),
            Node(':%s' % uuid_precpu_stats_throttling_data, {
                RDF.type: SwarmUI.ThrottlingData,
                Mu.uuid: uuid_precpu_stats_throttling_data,
                'swarmui:periods': data['precpu_stats']['throttling_data']['periods'],
                'swarmui:throttledPeriods': data['precpu_stats']['throttling_data']['throttled_periods'],
                'swarmui:throttledTime': data['precpu_stats']['throttling_data']['throttled_time'],
            }),
            Node(':%s' % uuid_precpu_stats, {
                RDF.type: SwarmUI.PrecpuStats,
                Mu.uuid: uuid_precpu_stats,
                'swarmui:cpuUsage': RDFTerm(':%s' % uuid_precpu_stats_cpu_usage),
                'swarmui:systemCpuUsage': data['precpu_stats'].get('system_cpu_usage'),
                'swarmui:throttlingData': RDFTerm(':%s' % uuid_precpu_stats_throttling_data),
                'swarmui:onlineCpus': data['precpu_stats'].get('online_cpus')
            }),
            Node(':%s' % uuid_memory_stats_stats, {
                RDF.type: SwarmUI.StatsInMemory,
                Mu.uuid: uuid_memory_stats_stats,
                'swarmui:activeAnon': data['memory_stats']['stats']['active_anon'],
                'swarmui:activeFile': data['memory_stats']['stats']['active_file'],
                'swarmui:cache': data['memory_stats']['stats']['cache'],
                'swarmui:dirty': data['memory_stats']['stats']['dirty'],
                'swarmui:hierarchicalMemoryLimit': data['memory_stats']['stats']['hierarchical_memory_limit'],
                'swarmui:inactiveAnon': data['memory_stats']['stats']['inactive_anon'],
                'swarmui:inactiveFile': data['memory_stats']['stats']['inactive_file'],
                'swarmui:mappedFile': data['memory_stats']['stats']['mapped_file'],
                'swarmui:pgfault': data['memory_stats']['stats']['pgfault'],
                'swarmui:pgmajfault': data['memory_stats']['stats']['pgmajfault'],
                'swarmui:pgpgin': data['memory_stats']['stats']['pgpgin'],
                'swarmui:pgpgout': data['memory_stats']['stats']['pgpgout'],
                'swarmui:rss': data['memory_stats']['stats']['rss'],
                'swarmui:rssHuge': data['memory_stats']['stats']['rss_huge'],
                'swarmui:totalActiveAnon': data['memory_stats']['stats']['total_active_anon'],
                'swarmui:totalActiveFile': data['memory_stats']['stats']['total_active_file'],
                'swarmui:totalCache': data['memory_stats']['stats']['total_cache'],
                'swarmui:totalDirty': data['memory_stats']['stats']['total_dirty'],
                'swarmui:totalInactiveAnon': data['memory_stats']['stats']['total_inactive_anon'],
                'swarmui:totalInactiveFile': data['memory_stats']['stats']['total_inactive_file'],
                'swarmui:totalMappedFile': data['memory_stats']['stats']['total_mapped_file'],
                'swarmui:totalPgfault': data['memory_stats']['stats']['total_pgfault'],
                'swarmui:totalPgmajfault': data['memory_stats']['stats']['total_pgmajfault'],
                'swarmui:totalPgpgin': data['memory_stats']['stats']['total_pgpgin'],
                'swarmui:totalPgpgout': data['memory_stats']['stats']['total_pgpgout'],
                'swarmui:totalRss': data['memory_stats']['stats']['total_rss'],
                'swarmui:totalRssHuge': data['memory_stats']['stats']['total_rss_huge'],
                'swarmui:totalUnevictable': data['memory_stats']['stats']['total_unevictable'],
                'swarmui:totalWriteback': data['memory_stats']['stats']['total_writeback'],
                'swarmui:unevictable': data['memory_stats']['stats']['unevictable'],
                'swarmui:writeback': data['memory_stats']['stats']['writeback'],
            }),
            Node(':%s' % uuid_memory_stats, {
                RDF.type: SwarmUI.MemoryStats,
                Mu.uuid: uuid_memory_stats,
                'swarmui:usage': data['memory_stats']['usage'],
                'swarmui:maxUsage': data['memory_stats']['max_usage'],
                'swarmui:statsInMemory': RDFTerm(':%s' % uuid_memory_stats_stats),
                'swarmui:limit': data['memory_stats']['limit'],
            }),
        ])
        await sparql.update("""
            PREFIX : <http://ontology.aksw.org/dockstats/>

            WITH {{graph}}
            INSERT DATA {
                {{}}
            }
            """, triples)

    logger.info("Finished logging stats (container %s is stopped)", container[:12])



@register_event
async def start_posting_elasticsearch(event: ContainerEvent, sparql: SPARQLClient):
    """
    Start posting docker stats into an ElasticSearch instance.
    """
    if not event.status == "start":
        return
    if not event.attributes.get('ELASTIC'):
        return

    try:
        es_host = ENV['ES_HOST']
    except KeyError:
        es_host = 'elasticsearch'
    try:
        es_port = ENV['ES_PORT']
    except KeyError:
        es_port = '9200'

    container = await event.container
    asyncio.ensure_future(save_container_elasticsearch(event.client, event.id, es_host, es_port))
    logger.info("Logging container %s in ElasticSearch", container['Id'][:12])


async def save_container_elasticsearch(client, container, es_host, es_port):
    """
    Post the docker stats JSON into an ElasticSearch instance.
    """
    es = Elasticsearch(["{host}:{port}".format(host=es_host, port=es_port)])
    async for data in client.stats(container, decode=True):
        es.index(index='docker', doc_type='stats', body=data)
    logger.info("Finished pushing stats into Elasticsearch (container %s is stopped)", container[:12])

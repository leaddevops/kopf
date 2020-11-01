"""
Keeping track of the cluster setup: namespaces, resources (custom and builtin).

The resource specifications can be partial or fuzzy (e.g. by categories),
the namespace specifications can be globs/patterns or negations of them.
The actual resources & namespaces in the cluster are permanently scanned
and matched against the specifications. Only those that do match are served.

Technically, it is implemented as a 2-dimensional matrix, with exact resources
and exact namespaces as the axes (``None`` for a cluster-wide pseudo-namespace).
Every cell in the matrix is a task for this combination of namespace & resource.
The tasks are started when new values are added to any dimension, or stopped
when some existing values are removed.

There are several kinds of tasks:

* Regular watchers (watch-streams) -- the main one.
* Peering watchers (watch-streams).
* Peering keep-alives (pingers).

The peering tasks are started only when the peering is enabled at all.
For peering, the resource is not used, only the namespace is of importance.

Some special watchers for the meta-level resources -- i.e. for dimensions --
are started and stopped separately, not as part of the the matrix.
"""
import asyncio
import functools
import itertools
import logging
from typing import Any, Collection, Dict, List, Mapping, \
                   MutableMapping, NamedTuple, Optional, TypeVar

from kopf.clients import errors, fetching, scanning
from kopf.engines import peering
from kopf.reactor import queueing, registries
from kopf.structs import bodies, configuration, handlers, primitives, references
from kopf.utilities import aiotasks

logger = logging.getLogger(__name__)


class WatcherKey(NamedTuple):
    resource: references.ResourceRef
    namespace: references.NamespaceRef


async def process_discovered_namespace_event(
        *,
        raw_event: bodies.RawEvent,
        namespaces: Collection[references.NamespaceSpec],
        insights: references.Insights,
        replenished: Optional[asyncio.Event] = None,
) -> None:
    if raw_event['type'] is None:
        return

    async with insights.revised:
        revise_namespaces(raw_events=[raw_event], insights=insights, namespaces=namespaces)
        insights.revised.notify_all()


async def process_discovered_resource_event(
        *,
        raw_event: bodies.RawEvent,
        registry: registries.OperatorRegistry,
        insights: references.Insights,
        replenished: Optional[asyncio.Event] = None,
) -> None:
    # Ignore the initial listing, as all custom resources were already noticed by API listing.
    # This prevents numerous unneccessary API requests at the the start of the operator.
    if raw_event['type'] is None:
        return

    # Re-scan the whole dimension of resources if any single one of them changes. By this, we make
    # K8s's /apis/ endpoint the source of truth for all resources & versions & preferred versions,
    # instead of mimicking K8s in interpreting them ourselves (a probable source of bugs).
    # As long as it is atomic (for asyncio, i.e. sync), the existing tasks will not be affected.
    group = raw_event['object']['spec']['group']
    resources = await scanning.scan_resources(groups={group})
    async with insights.revised:
        revise_resources(resources=resources, insights=insights, registry=registry, group=group)
        insights.backbone.fill(resources=resources)
        insights.revised.notify_all()


async def ochestrator(
        *,
        processor: queueing.WatchStreamProcessor,
        settings: configuration.OperatorSettings,
        identity: peering.Identity,
        insights: references.Insights,
) -> None:

    # Multidimensional tasks -- one for every combination of relevant dimensions.
    watcher_tasks: Dict[WatcherKey, aiotasks.Task] = {}
    peering_tasks: Dict[WatcherKey, aiotasks.Task] = {}
    pinging_tasks: Dict[WatcherKey, aiotasks.Task] = {}

    # Multidimentional freeze: for every namespace, and a few for the whole cluster (for CRDs).
    freeze_checker = primitives.ToggleSet()
    freeze_blocker = await freeze_checker.make_toggle(name='peering CRD is absent')
    freeze_toggles: Dict[WatcherKey, primitives.Toggle] = {}

    try:
        async with insights.revised:
            while True:
                await insights.revised.wait()
                await adjust_tasks(
                    processor=processor,
                    insights=insights,
                    settings=settings,
                    identity=identity,
                    watcher_tasks=watcher_tasks,
                    peering_tasks=peering_tasks,
                    pinging_tasks=pinging_tasks,
                    freeze_blocker=freeze_blocker,
                    freeze_checker=freeze_checker,
                    freeze_toggles=freeze_toggles,
                )
    except asyncio.CancelledError:
        tasks = (frozenset(watcher_tasks.values()) |
                 frozenset(peering_tasks.values()) |
                 frozenset(pinging_tasks.values()))
        await aiotasks.stop(tasks, title="streaming", logger=logger, interval=10)
        raise


async def adjust_tasks(
        *,
        processor: queueing.WatchStreamProcessor,
        insights: references.Insights,
        settings: configuration.OperatorSettings,
        identity: peering.Identity,
        watcher_tasks: Dict[WatcherKey, aiotasks.Task],
        peering_tasks: Dict[WatcherKey, aiotasks.Task],
        pinging_tasks: Dict[WatcherKey, aiotasks.Task],
        freeze_toggles: Dict[WatcherKey, primitives.Toggle],
        freeze_blocker: primitives.Toggle,
        freeze_checker: primitives.ToggleSet,
) -> None:
    """
    Stop & start the tasks to match the task matrix with the cluster insights.

    As a rule of thumb, stop the tasks first, start later -- not vice versa!
    """
    peering_spec = peering.guess_resource(settings=settings)
    peering_resource = insights.backbone.get(peering_spec) if peering_spec is not None else None
    peering_resources = [peering_resource] if peering_resource is not None else []

    # Freeze or resume all streams if the peering CRDs are absent but required.
    # Ignore the CRD absence in auto-detection mode: freeze only when (and if) the CRDs are added.
    await freeze_blocker.turn_to(settings.peering.mandatory and not peering_resources)

    # Do not distinguish the keys: even for the case when the peering CRD is served by the operator,
    # for the peering CRD or namespace deletion, both tasks are stopped together, never apart.
    remaining_namespaces = {None} | insights.namespaces
    redundant_keys: Collection[WatcherKey] = (
            {key for key in watcher_tasks if key.namespace not in remaining_namespaces} |
            {key for key in peering_tasks if key.namespace not in remaining_namespaces} |
            {key for key in pinging_tasks if key.namespace not in remaining_namespaces} |
            {key for key in freeze_toggles if key.namespace not in remaining_namespaces} |
            {key for key in watcher_tasks if key.resource not in insights.resources} |
            {key for key in peering_tasks if key.resource not in peering_resources} |
            {key for key in pinging_tasks if key.resource not in peering_resources} |
            {key for key in freeze_toggles if key.resource not in peering_resources} |
            set())
    redundant_tasks = _get_keys(redundant_keys, watcher_tasks, peering_tasks, pinging_tasks)
    redundant_toggles = _get_keys(redundant_keys, freeze_toggles)
    _del_keys(redundant_keys, watcher_tasks, peering_tasks, pinging_tasks, freeze_toggles)

    # Release the resources, stop the tasks -- in parallel, and after the states are cleaned.
    for redundant_toggle in redundant_toggles:
        await freeze_checker.drop_toggle(redundant_toggle)
    await aiotasks.stop(redundant_tasks, title="streaming", logger=logger, interval=10, quiet=True)

    # Start the peering tasks before the regular tasks: so that the freeze (if any) is pre-enabled.
    for resource, namespace in itertools.product(peering_resources, insights.namespaces):
        dkey = WatcherKey(resource=resource, namespace=namespace)
        if dkey not in peering_tasks:
            what = f"{settings.peering.name}@{namespace}"
            freeze_toggle = await freeze_checker.make_toggle(settings.peering.mandatory, name=what)
            freeze_toggles[dkey] = freeze_toggle
            pinging_tasks[dkey] = aiotasks.create_guarded_task(
                name=f"peering keep-alive for {what}", logger=logger, cancellable=True,
                coro=peering.keepalive(
                    namespace=namespace,
                    resource=resource,
                    settings=settings,
                    identity=identity))
            peering_tasks[dkey] = aiotasks.create_guarded_task(
                name=f"peering observer for {what}", logger=logger, cancellable=True,
                coro=queueing.watcher(
                    freeze_checker=None,
                    settings=settings,
                    resource=resource,
                    namespace=namespace,
                    processor=functools.partial(peering.process_peering_event,
                                                freeze_toggle=freeze_toggle,
                                                namespace=namespace,
                                                resource=resource,
                                                settings=settings,
                                                identity=identity)))

    # Start the watchers for newly appeared dimensions.
    for resource, namespace in itertools.product(insights.resources, insights.namespaces):
        namespace = namespace if resource.namespaced else None
        dkey = WatcherKey(resource=resource, namespace=namespace)
        if dkey not in watcher_tasks:
            what = f"{resource}@{namespace}"
            watcher_tasks[dkey] = aiotasks.create_guarded_task(
                name=f"watcher for {what}", logger=logger, cancellable=True,
                coro=queueing.watcher(
                    freeze_checker=freeze_checker,
                    settings=settings,
                    resource=resource,
                    namespace=namespace,
                    processor=functools.partial(processor, resource=resource)))


def revise_namespaces(
        *,
        insights: references.Insights,
        namespaces: Collection[references.NamespaceSpec],
        raw_events: Collection[bodies.RawEvent] = (),
        raw_bodies: Collection[bodies.RawBody] = (),
) -> None:
    all_events = list(raw_events) + [bodies.RawEvent(type=None, object=obj) for obj in raw_bodies]
    for raw_event in all_events:
        namespace = references.NamespaceName(raw_event['object']['metadata']['name'])
        matched = any(references.match_namespace(namespace, spec) for spec in namespaces)
        deleted = is_deleted(raw_event)
        if deleted:
            insights.namespaces.discard(namespace)
        elif matched:
            insights.namespaces.add(namespace)


def revise_resources(
        *,
        group: Optional[str],
        insights: references.Insights,
        registry: registries.OperatorRegistry,
        resources: Collection[references.ResourceRef],
) -> None:

    # Scan only the resource-related handlers, ignore activies & co.
    resource_handlers: List[handlers.ResourceHandler] = []
    resource_handlers.extend(registry.resource_watching_handlers.get_all_handlers())
    resource_handlers.extend(registry.resource_changing_handlers.get_all_handlers())
    resource_handlers.extend(registry.resource_spawning_handlers.get_all_handlers())
    # r1: List[handlers.ResourceHandler] = sum((list(r.get_all_handlers()) for r in registry.resource_watching_handlers.values()), [])
    # r2: List[handlers.ResourceHandler] = sum((list(r.get_all_handlers()) for r in registry.resource_watching_handlers.values()), [])
    # r3: List[handlers.ResourceHandler] = sum((list(r.get_all_handlers()) for r in registry.resource_watching_handlers.values()), [])
    # resource_handlers: List[handlers.ResourceHandler] = r1 + r2 + r3
    resource_specs = {handler.resource for handler in resource_handlers if handler.resource is not None}
    resources_sss = {spec: spec.select(resources, all=False) for spec in resource_specs}

    if group is None:
        insights.resources.clear()
    else:
        group_resources = {resource for resource in insights.resources if resource.group == group}
        insights.resources.difference_update(group_resources)

    # Stop watching a CRD when it is deleted. However, we don't block the CRDs with finalizers, so
    # it can be so that we miss the event and continue watching attempts (and fail with HTTP 404).
    # Also stop watching the resources that were changed to not hit any specs now (e.g. categories).
    for spec, refs in resources_sss.items():
        insights.resources.update(refs)

    # Detect ambiguous specs and stop watching: 2+ distinct resources for the same resource spec.
    # E.g.: "pods.v1" & "pods.v1beta1.metrics.k8s.io", when specified as just "pods" (but only
    # if non-v1 resources cannot be filtered out completely; otherwise, implicitly prefer v1).
    resolved = {spec: spec.select(insights.resources, all=True) for spec in resource_specs}
    for spec, resources in resolved.items():
        if spec.is_specific and len(resources) > 1:
            logger.warning("Ambiguous resources will not be served (try specifying API groups):"
                           f" {spec} => {resources}")
            insights.resources.difference_update(resources)

    # Warn for handlers that specify unexistent resources (maybe a typo or a misconfiguration).
    unresolved_specs = resource_specs - {spec for spec, resources in resolved.items() if resources}
    unresolved_names = ', '.join(f"{spec}" for spec in unresolved_specs)
    if unresolved_specs:
        logger.warning("Unresolved resources cannot be served (try creating their CRDs):"
                       f" {unresolved_names}")


async def resource_observer(
        *,
        settings: configuration.OperatorSettings,
        registry: registries.OperatorRegistry,
        insights: references.Insights,
) -> None:

    # Scan only the resource-related handlers, ignore activies & co.
    resource_handlers: List[handlers.ResourceHandler] = []
    resource_handlers.extend(registry.resource_watching_handlers.get_all_handlers())
    resource_handlers.extend(registry.resource_changing_handlers.get_all_handlers())
    resource_handlers.extend(registry.resource_spawning_handlers.get_all_handlers())
    # r1: List[handlers.ResourceHandler] = sum((list(r.get_all_handlers()) for r in registry.resource_watching_handlers.values()), [])
    # r2: List[handlers.ResourceHandler] = sum((list(r.get_all_handlers()) for r in registry.resource_watching_handlers.values()), [])
    # r3: List[handlers.ResourceHandler] = sum((list(r.get_all_handlers()) for r in registry.resource_watching_handlers.values()), [])
    # resource_handlers: List[handlers.ResourceHandler] = r1 + r2 + r3
    groups = {handler.resource.group for handler in resource_handlers if handler.resource is not None}
    groups.update({resource.group for resource in {
        references.NAMESPACES, references.CRDS, references.EVENTS, references.CLUSTER_PEERINGS, references.NAMESPACED_PEERINGS,
    }})

    # Prepopulate the resources before the dimension watchers start, so that each initially listed
    # namespace would start a watcher, and each initially listed CRD is already on the list.
    group_filter = None if None in groups else {group for group in groups if group is not None}
    resources = await scanning.scan_resources(groups=group_filter)
    async with insights.revised:
        revise_resources(resources=resources, insights=insights, registry=registry, group=None)
        insights.backbone.fill(resources=resources)
        insights.revised.notify_all()

    # Notify those waiting for the initial listing (e.g. CLI commands).
    insights.ready_resources.set()

    # Wait for the actual resource for CRDs to be found in the initial cluster scanning.
    async with insights.backbone.revised:
        await insights.backbone.revised.wait_for(lambda: references.CRDS in insights.backbone)

    # The resource watcher starts with an initial listing, and later reacts to any changes. However,
    # the existing resources are known already, so there will be no changes on the initial listing.
    if not settings.scanning.disabled:
        try:
            await queueing.watcher(
                freeze_checker=None,
                settings=settings,
                resource=insights.backbone[references.CRDS],
                namespace=None,
                processor=functools.partial(process_discovered_resource_event,
                                            registry=registry,
                                            insights=insights))
        except errors.APIForbiddenError:
            logger.warning("Not enough permissions to watch for resources: "
                           "changes (creation/deletion/updates) will not be noticed. "
                           "To refresh the resources, the operator must be restarted.")
            await asyncio.Event().wait()
    else:
        await asyncio.Event().wait()


async def namespace_observer(
        *,
        clusterwide: bool,
        namespaces: Collection[references.NamespaceSpec],
        insights: references.Insights,
        settings: configuration.OperatorSettings,
) -> None:
    exact_namespaces = references.select_specific_namespaces(namespaces)

    # Wait for the actual resource for namespaces to be found in the initial cluster scanning.
    async with insights.backbone.revised:
        await insights.backbone.revised.wait_for(lambda: references.NAMESPACES in insights.backbone)

    # Populate the namespaces atomically (instead of notifying on every item from the watch-stream).
    if not settings.scanning.disabled and not clusterwide:
        try:
            resource = insights.backbone[references.NAMESPACES]
            objs, _ = await fetching.list_objs_rv(resource=resource, namespace=None)
            async with insights.revised:
                revise_namespaces(raw_bodies=objs, insights=insights, namespaces=namespaces)
                insights.revised.notify_all()
        except errors.APIForbiddenError:
            logger.warning("Not enough permissions to list namespaces. "
                           "Falling back to a list of namespaces which are assumed to exist: "
                           f"{exact_namespaces!r}")
            async with insights.revised:
                insights.namespaces.update(exact_namespaces)
                insights.revised.notify_all()
    else:
        async with insights.revised:
            insights.namespaces.update({None} if clusterwide else exact_namespaces)
            insights.revised.notify_all()

    # Notify those waiting for the initial listing (e.g. CLI commands).
    insights.ready_namespaces.set()

    if not settings.scanning.disabled and not clusterwide:
        try:
            await queueing.watcher(
                freeze_checker=None,
                settings=settings,
                resource=insights.backbone[references.NAMESPACES],
                namespace=None,
                processor=functools.partial(process_discovered_namespace_event,
                                            namespaces=namespaces,
                                            insights=insights))
        except errors.APIForbiddenError:
            logger.warning("Not enough permissions to watch for namespaces: "
                           "changes (deletion/creation) will not be noticed."
                           "To refresh the namespaces, the operator must be restarted.")
            await asyncio.Event().wait()
    else:
        await asyncio.Event().wait()


def is_deleted(raw_event: bodies.RawEvent) -> bool:
    marked_as_deleted = bool(raw_event['object'].get('metadata', {}).get('deletionTimestamp'))
    really_is_deleted = raw_event['type'] == 'DELETED'
    return marked_as_deleted or really_is_deleted


# Module-specific overspecialised helpers to manipulate dicts of tasks & toggles.
K = TypeVar('K')
V = TypeVar('V')


def _get_keys(keys: Collection[K], *ds: Mapping[K, V]) -> Collection[V]:
    result: List[V] = []
    for d in ds:
        for key in keys:
            try:
                result.append(d[key])
            except KeyError:
                pass
    return result


def _del_keys(keys: Collection[K], *ds: MutableMapping[K, Any]) -> None:
    for d in ds:
        for key in keys:
            try:
                del d[key]
            except KeyError:
                pass

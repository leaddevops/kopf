"""
Peer monitoring: knowing which other operators do run, and exchanging the basic signals with them.

The main use-case is to suppress all deployed operators when a developer starts a dev-/debug-mode
operator for the same cluster on their workstation -- to avoid any double-processing.

See also: `kopf freeze` & `kopf resume` CLI commands for the same purpose.

WARNING: There are **NO** per-object locks between the operators, so only one operator
should be functional for the cluster, i.e. only one with the highest priority running.
If the operator sees the violations of this constraint, it will print the warnings
pointing to another same-priority operator, but will continue to function.

The "signals" exchanged are only the keep-alive notifications from the operator being alive,
and detection of other operators hard termination (by timeout rather than by clear exit).

The peers monitoring covers both the in-cluster operators running,
and the dev-mode operators running in the dev workstations.

For this, special CRDs ``kind: ClusterKopfPeering`` & ``kind: KopfPeering``
should be registered in the cluster, and their ``status`` field is used
by all the operators to sync their keep-alive info.

The namespace-bound operators (e.g. `--namespace=`) report their individual
namespaces are part of the payload, can see all other cluster and namespaced
operators (even from the different namespaces), and behave accordingly.

The CRD is not applied automatically, so you have to deploy it yourself explicitly.
To disable the peers monitoring, use the `--standalone` CLI option.
"""

import asyncio
import datetime
import getpass
import logging
import os
import random
from typing import Any, Dict, Iterable, Mapping, NewType, NoReturn, Optional, cast

import click
import iso8601

from kopf.clients import patching
from kopf.structs import bodies, configuration, patches, primitives, references
from kopf.utilities import aiotasks, hostnames

logger = logging.getLogger(__name__)

Identity = NewType('Identity', str)


# The class used to represent a peer in the parsed peers list (for convenience).
# The extra fields are for easier calculation when and if the peer is dead to the moment.
class Peer:

    def __init__(
            self,
            *,
            identity: Identity,
            priority: int = 0,
            lifetime: int = 60,
            lastseen: Optional[str] = None,
            **_: Any,  # for the forward-compatibility with the new fields
    ):
        super().__init__()
        self.identity = identity
        self.priority = priority
        self.lifetime = datetime.timedelta(seconds=int(lifetime))
        self.lastseen = (iso8601.parse_date(lastseen) if lastseen is not None else
                         datetime.datetime.utcnow())
        self.lastseen = self.lastseen.replace(tzinfo=None)  # only the naive utc -- for comparison
        self.deadline = self.lastseen + self.lifetime
        self.is_dead = self.deadline <= datetime.datetime.utcnow()

    def __repr__(self) -> str:
        clsname = self.__class__.__name__
        options = ", ".join(f"{key!s}={val!r}" for key, val in self.as_dict().items())
        return f"<{clsname} {self.identity}: {options}>"

    def as_dict(self) -> Dict[str, Any]:
        # Only the non-calculated and non-identifying fields.
        return {
            'priority': int(self.priority),
            'lifetime': int(self.lifetime.total_seconds()),
            'lastseen': str(self.lastseen.isoformat()),
        }


async def process_peering_event(
        *,
        raw_event: bodies.RawEvent,
        namespace: references.NamespaceRef,
        resource: references.ResourceRef,
        settings: configuration.OperatorSettings,
        identity: Identity,
        autoclean: bool = True,
        replenished: asyncio.Event,
        freeze_toggle: primitives.Toggle,
) -> None:
    """
    Handle a single update of the peers by us or by other operators.

    When an operator with a higher priority appears, switch to the freeze-mode.
    The these operators disappear or become presumably dead, resume the event handling.

    The freeze object is passed both to the peers handler to set/clear it,
    and to all the resource handlers to check its value when the events arrive
    (see `create_tasks` and `run` functions).
    """
    body: bodies.RawBody = raw_event['object']
    meta: bodies.RawMeta = raw_event['object']['metadata']

    # Silently ignore the peering objects which are not ours to worry.
    if meta.get('name') != settings.peering.name:
        return

    # Find if we are still the highest priority operator.
    pairs = cast(Mapping[str, Mapping[str, object]], body.get('status', {}))
    peers = [Peer(identity=Identity(opid), **opinfo) for opid, opinfo in pairs.items()]
    dead_peers = [peer for peer in peers if peer.is_dead]
    live_peers = [peer for peer in peers if not peer.is_dead and peer.identity != identity]
    prio_peers = [peer for peer in live_peers if peer.priority > settings.peering.priority]
    same_peers = [peer for peer in live_peers if peer.priority == settings.peering.priority]

    if autoclean and dead_peers:
        await clean(peers=dead_peers, settings=settings, namespace=namespace, resource=resource)

    if prio_peers:
        if freeze_toggle.is_off():
            logger.info(f"Freezing operations in favour of {prio_peers}.")
            await freeze_toggle.turn_to(True)
    elif same_peers:
        logger.warning(f"Possibly conflicting operators with the same priority: {same_peers}.")
        if freeze_toggle.is_off():
            logger.warning(f"Freezing all operators, including self: {peers}")
            await freeze_toggle.turn_to(True)
    else:
        if freeze_toggle.is_on():
            logger.info(f"Resuming operations after the freeze. Conflicting operators with the same priority are gone.")
            await freeze_toggle.turn_to(False)

    # Either wait for external updates (and exit when they arrive), or until the blocking peers
    # are expected to expire, and force the immediate re-evaluation by a certain change of self.
    # This incurs an extra PATCH request besides usual keepalives, but in the complete silence
    # from other peers that existed a moment earlier, this should not be a problem.
    now = datetime.datetime.utcnow()
    delay = max([0] + [(peer.deadline - now).total_seconds() for peer in same_peers + prio_peers])
    if delay:
        try:
            await asyncio.wait_for(replenished.wait(), timeout=delay)
        except asyncio.TimeoutError:
            await touch(
                identity=identity,
                settings=settings,
                resource=resource,
                namespace=namespace,
            )


async def keepalive(
        *,
        namespace: references.NamespaceRef,
        resource: references.ResourceRef,
        identity: Identity,
        settings: configuration.OperatorSettings,
) -> NoReturn:
    """
    An ever-running coroutine to regularly send our own keep-alive status for the peers.
    """
    try:
        while True:
            await touch(
                identity=identity,
                settings=settings,
                resource=resource,
                namespace=namespace,
            )

            # How often do we update. Keep limited to avoid k8s api flooding.
            # Should be slightly less than the lifetime, enough for a patch request to finish.
            # A little jitter is added to evenly distribute the keep-alives over time.
            lifetime = settings.peering.lifetime
            duration = min(lifetime, max(1, lifetime - random.randint(5, 10)))
            await asyncio.sleep(max(1, duration))
    finally:
        try:
            await asyncio.shield(touch(
                identity=identity,
                settings=settings,
                resource=resource,
                namespace=namespace,
                lifetime=0,
            ))
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception(f"Couldn't remove self from the peering. Ignoring.")


async def touch(
        *,
        namespace: references.NamespaceRef,
        resource: references.ResourceRef,
        settings: configuration.OperatorSettings,
        identity: Identity,
        lifetime: Optional[int] = None,
) -> None:
    name = settings.peering.name
    peer = Peer(
        identity=identity,
        priority=settings.peering.priority,
        lifetime=settings.peering.lifetime if lifetime is None else lifetime,
    )

    patch = patches.Patch()
    patch.update({'status': {identity: None if peer.is_dead else peer.as_dict()}})
    rsp = await patching.patch_obj(resource=resource, namespace=namespace, name=name, patch=patch)

    if not settings.peering.stealth or rsp is None:
        where = f"in {namespace!r}" if namespace else "cluster-wide"
        result = "not found" if rsp is None else "ok"
        logger.debug(f"Keep-alive in {name!r} {where}: {result}.")


async def clean(
        *,
        namespace: references.NamespaceRef,
        resource: references.ResourceRef,
        settings: configuration.OperatorSettings,
        peers: Iterable[Peer],
) -> None:
    name = settings.peering.name

    patch = patches.Patch()
    patch.update({'status': {peer.identity: None for peer in peers}})
    await patching.patch_obj(resource=resource, namespace=namespace, name=name, patch=patch)


def detect_own_id(*, manual: bool) -> Identity:
    """
    Detect or generate the id for ourselves, i.e. the execute operator.

    It is constructed easy to detect in which pod it is running
    (if in the cluster), or who runs the operator (if not in the cluster,
    i.e. in the dev-mode), and how long ago was it started.

    The pod id can be specified by::

        env:
        - name: POD_ID
          valueFrom:
            fieldRef:
              fieldPath: metadata.name

    Used in the `kopf.reactor.queueing` when the reactor starts,
    but is kept here, close to the rest of the peering logic.
    """

    pod = os.environ.get('POD_ID', None)
    if pod is not None:
        return Identity(pod)

    user = getpass.getuser()
    host = hostnames.get_descriptive_hostname()
    now = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")
    rnd = ''.join(random.choices('abcdefhijklmnopqrstuvwxyz0123456789', k=3))
    return Identity(f'{user}@{host}' if manual else f'{user}@{host}/{now}/{rnd}')


def guess_resource(settings: configuration.OperatorSettings) -> Optional[references.ResourceSpec]:
    if settings.peering.standalone:
        return None
    elif settings.peering.clusterwide:
        return references.CLUSTER_PEERINGS
    elif settings.peering.namespaced:
        return references.NAMESPACED_PEERINGS
    else:
        raise TypeError("Unidentified peering mode (none of standalone/cluster/namespaced).")


async def touch_command(
        *,
        lifetime: Optional[int],
        insights: references.Insights,
        identity: Identity,
        settings: configuration.OperatorSettings,
) -> None:

    await asyncio.wait({
        insights.ready_namespaces.wait(),
        insights.ready_resources.wait(),
    })

    resource_spec = guess_resource(settings=settings)
    resource = insights.backbone.get(resource_spec) if resource_spec else None
    if resource is None:
        raise click.UsageError(f"Cannot find the peering resource {resource_spec}.")

    await aiotasks.wait({
        aiotasks.create_guarded_task(
            name="peering command", finishable=True, logger=logger,
            coro=touch(
                namespace=namespace,
                resource=resource,
                identity=identity,
                settings=settings,
                lifetime=lifetime),
        )
        for namespace in insights.namespaces
    })

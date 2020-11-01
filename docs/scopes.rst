======
Scopes
======


Namespaces
==========

The running operator can be restricted to handle custom resources
in one namespace only::

    kopf run --namespace=some-namespace ...
    kopf run -n some-namespace ...

Multiple namespaces can be passed::

    kopf run --namespace=some-namespace --namespace=another-namespace ...
    kopf run -n some-namespace -n another-namespace ...

Namespace globs can be used too::

    kopf run --namespace=*-pr-123-* ...
    kopf run -n *-pr-123-* ...

In all cases, the operator monitors the namespaces that exist in the cluster,
or are created/deleted at runtime, and starts/stops serving them accordingly.

If there are no permissions to list/watch the namespaces, the operator falls
back to the list of provided namespaces "as is", assuming they exist.
If the namespaces do not exist, this might lead to errors (HTTP 404 Not Found).
Namespace patterns do not work in this case; only the specific namespaces do.


Cluster-wide
============

To serve the resources in the whole cluster::

    kopf run --all-namespaces ...
    kopf run -A ...

In that case, the operator does not monitor the namespaces in the cluster,
and uses different K8s API URLs to list/watch the objects cluster-wide.

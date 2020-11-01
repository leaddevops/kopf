import pytest

from kopf.structs.references import CLUSTER_PEERINGS_, NAMESPACED_PEERINGS_


@pytest.fixture(autouse=True)
def _autouse_fake_vault(fake_vault):
    pass


@pytest.fixture()
def with_cluster_crd(hostname, aresponses):
    result = {'resources': [{
        'name': CLUSTER_PEERINGS_.plural,
        'namespaced': False,
    }]}
    url = CLUSTER_PEERINGS_.get_version_url()
    aresponses.add(hostname, url, 'get', result)


@pytest.fixture()
def with_namespaced_crd(hostname, aresponses):
    result = {'resources': [{
        'name': NAMESPACED_PEERINGS_.plural,
        'namespaced': True,
    }]}
    url = NAMESPACED_PEERINGS_.get_version_url()
    aresponses.add(hostname, url, 'get', result)


@pytest.fixture()
def with_both_crds(hostname, aresponses):
    result = {'resources': [{
        'name': CLUSTER_PEERINGS_.plural,
        'namespaced': False,
    }, {
        'name': NAMESPACED_PEERINGS_.plural,
        'namespaced': True,
    }]}
    urls = {
        CLUSTER_PEERINGS_.get_version_url(),
        NAMESPACED_PEERINGS_.get_version_url(),
    }
    for url in urls:
        aresponses.add(hostname, url, 'get', result)

from common.volume import Volume
from common.hdl import MayastorHandle
import logging
import pytest
import subprocess
import time
import uuid as guid
import mayastor_pb2 as pb
from common.fio import Fio
from common.nvme import (
    nvme_connect,
    nvme_disconnect,
    nvme_list_subsystems,
    nvme_resv_report,
)

from common.mayastor import containers_mod, mayastor_mod


@pytest.fixture
def create_nexus(
    mayastor_mod, nexus_name, nexus_uuid, create_replica, min_cntlid, resv_key
):
    """ Create a nexus on ms3 with 2 replicas """
    hdls = mayastor_mod
    replicas = create_replica
    replicas = [k.uri for k in replicas]

    NEXUS_UUID, size_mb = nexus_uuid
    NEXUS_NAME = nexus_name

    hdls["ms3"].nexus_create_v2(
        NEXUS_NAME,
        NEXUS_UUID,
        size_mb,
        min_cntlid,
        min_cntlid + 9,
        resv_key,
        replicas,
    )
    uri = hdls["ms3"].nexus_publish(NEXUS_NAME)
    assert len(hdls["ms1"].bdev_list()) == 2
    assert len(hdls["ms2"].bdev_list()) == 2
    assert len(hdls["ms3"].bdev_list()) == 1

    assert len(hdls["ms1"].pool_list().pools) == 1
    assert len(hdls["ms2"].pool_list().pools) == 1

    yield uri
    hdls["ms3"].nexus_destroy(NEXUS_NAME)


@pytest.fixture
def create_nexus_2(mayastor_mod, nexus_name, nexus_uuid, min_cntlid_2, resv_key_2):
    """ Create a 2nd nexus on ms0 with the same 2 replicas but with resv_key_2 """
    hdls = mayastor_mod
    NEXUS_NAME = nexus_name

    replicas = []
    list = mayastor_mod.get("ms3").nexus_list_v2()
    nexus = next(n for n in list if n.name == NEXUS_NAME)
    replicas.append(nexus.children[0].uri)
    replicas.append(nexus.children[1].uri)

    NEXUS_UUID, size_mb = nexus_uuid

    hdls["ms0"].nexus_create_v2(
        NEXUS_NAME,
        NEXUS_UUID,
        size_mb,
        min_cntlid_2,
        min_cntlid_2 + 9,
        resv_key_2,
        replicas,
    )
    uri = hdls["ms0"].nexus_publish(NEXUS_NAME)
    assert len(hdls["ms0"].bdev_list()) == 1
    assert len(hdls["ms1"].bdev_list()) == 2
    assert len(hdls["ms2"].bdev_list()) == 2
    assert len(hdls["ms3"].bdev_list()) == 1

    yield uri
    hdls["ms0"].nexus_destroy(NEXUS_NAME)


@pytest.fixture
def create_nexus_dev(create_nexus):
    uri = create_nexus
    dev = nvme_connect(uri)
    yield dev
    nvme_disconnect(uri)


@pytest.fixture
def create_nexus_2_dev(create_nexus_2):
    uri = create_nexus_2
    dev = nvme_connect(uri)
    yield dev
    nvme_disconnect(uri)


@pytest.fixture
def create_nexus_3_dev(
    mayastor_mod, nexus_name, nexus_uuid, replica_uuid, min_cntlid_3, resv_key_3
):
    """ Create a 3rd nexus on ms1 with the same 2 replicas but with resv_key_3 """
    hdls = mayastor_mod
    NEXUS_NAME = nexus_name

    replicas = []
    list = mayastor_mod.get("ms3").nexus_list_v2()
    nexus = next(n for n in list if n.name == NEXUS_NAME)
    # use loopback until nvme initiator can connect to target in same instance
    REP_UUID, rep_size_mb = replica_uuid
    replicas.append("loopback:///" + REP_UUID)
    replicas.append(nexus.children[1].uri)

    NEXUS_UUID, size_mb = nexus_uuid

    hdls["ms1"].nexus_create_v2(
        NEXUS_NAME,
        NEXUS_UUID,
        size_mb,
        min_cntlid_3,
        min_cntlid_3 + 9,
        resv_key_3,
        replicas,
    )
    uri = hdls["ms1"].nexus_publish(NEXUS_NAME)
    assert len(hdls["ms0"].bdev_list()) == 1
    assert len(hdls["ms1"].bdev_list()) == 3
    assert len(hdls["ms2"].bdev_list()) == 2
    assert len(hdls["ms3"].bdev_list()) == 1

    dev = nvme_connect(uri)

    yield dev
    nvme_disconnect(uri)
    hdls["ms1"].nexus_destroy(NEXUS_NAME)


@pytest.fixture
def pool_config():
    """
    The idea is this used to obtain the pool types and names that should be
    created.
    """
    pool = {}
    pool["name"] = "tpool"
    pool["uri"] = "malloc:///disk0?size_mb=100"
    return pool


@pytest.fixture
def replica_uuid():
    """Replica UUID to be used."""
    UUID = "0000000-0000-0000-0000-000000000001"
    size_mb = 64 * 1024 * 1024
    return (UUID, size_mb)


@pytest.fixture
def nexus_name():
    """Nexus name to be used."""
    NEXUS_NAME = "nexus0"
    return NEXUS_NAME


@pytest.fixture
def nexus_uuid():
    """Nexus UUID to be used."""
    NEXUS_UUID = "3ae73410-6136-4430-a7b5-cbec9fe2d273"
    size_mb = 64 * 1024 * 1024
    return (NEXUS_UUID, size_mb)


@pytest.fixture
def min_cntlid():
    """NVMe minimum controller ID to be used."""
    min_cntlid = 50
    return min_cntlid


@pytest.fixture
def min_cntlid_2():
    """NVMe minimum controller ID to be used for 2nd nexus."""
    min_cntlid = 60
    return min_cntlid


@pytest.fixture
def min_cntlid_3():
    """NVMe minimum controller ID for 3rd nexus."""
    min_cntlid = 70
    return min_cntlid


@pytest.fixture
def resv_key():
    """NVMe reservation key to be used."""
    resv_key = 0xABCDEF0012345678
    return resv_key


@pytest.fixture
def resv_key_2():
    """NVMe reservation key to be used for 2nd nexus."""
    resv_key = 0x1234567890ABCDEF
    return resv_key


@pytest.fixture
def resv_key_3():
    """NVMe reservation key for 3rd nexus."""
    resv_key = 0x567890ABCDEF1234
    return resv_key


@pytest.fixture
def create_pools(mayastor_mod, pool_config):
    hdls = mayastor_mod

    cfg = pool_config
    pools = []

    pools.append(hdls["ms1"].pool_create(cfg.get("name"), cfg.get("uri")))

    pools.append(hdls["ms2"].pool_create(cfg.get("name"), cfg.get("uri")))

    for p in pools:
        assert p.state == pb.POOL_ONLINE
    yield pools
    try:
        hdls["ms1"].pool_destroy(cfg.get("name"))
        hdls["ms2"].pool_destroy(cfg.get("name"))
    except Exception:
        pass


@pytest.fixture
def create_replica(mayastor_mod, replica_uuid, create_pools):
    hdls = mayastor_mod
    pools = create_pools
    replicas = []

    UUID, size_mb = replica_uuid

    replicas.append(hdls["ms1"].replica_create(pools[0].name, UUID, size_mb))
    replicas.append(hdls["ms2"].replica_create(pools[0].name, UUID, size_mb))

    yield replicas
    try:
        hdls["ms1"].replica_destroy(UUID)
        hdls["ms2"].replica_destroy(UUID)
    except Exception as e:
        logging.debug(e)


@pytest.fixture
def start_fio(create_nexus_dev):
    dev = create_nexus_dev
    cmd = Fio("job1", "randwrite", dev).build().split()
    output = subprocess.Popen(cmd)
    # wait for fio to start
    time.sleep(1)
    yield
    output.communicate()
    assert output.returncode == 0


@pytest.mark.timeout(60)
def test_nexus_multipath(
    create_nexus,
    create_nexus_2,
    nexus_name,
    nexus_uuid,
    mayastor_mod,
    resv_key,
    resv_key_2,
):
    """Create 2 nexuses, each with 2 replicas, with different NVMe reservation keys"""

    uri = create_nexus
    uri2 = create_nexus_2
    NEXUS_UUID, _ = nexus_uuid
    NEXUS_NAME = nexus_name
    resv_key = resv_key
    resv_key_2 = resv_key_2

    list = mayastor_mod.get("ms3").nexus_list_v2()
    nexus = next(n for n in list if n.name == NEXUS_NAME)
    assert nexus.uuid == NEXUS_UUID

    for c in range(2):
        child_uri = nexus.children[c].uri

        dev = nvme_connect(child_uri)
        report = nvme_resv_report(dev)

        assert (
            report["rtype"] == 5
        ), "should have write exclusive, all registrants reservation"
        assert report["regctl"] == 2, "should have 2 registered controllers"
        assert report["ptpls"] == 0, "should have Persist Through Power Loss State of 0"
        for i in range(2):
            assert (
                report["regctlext"][i]["cntlid"] == 0xFFFF
            ), "should have dynamic controller ID"
        assert report["regctlext"][0]["rkey"] == resv_key
        assert report["regctlext"][1]["rkey"] == resv_key_2
        assert report["regctlext"][0]["rcsts"] == 1
        assert report["regctlext"][1]["rcsts"] == 0

        nvme_disconnect(child_uri)


@pytest.mark.timeout(60)
def test_nexus_multipath_add_3rd_path(
    create_nexus_dev,
    create_nexus_2_dev,
    start_fio,
    create_nexus_3_dev,
):
    """Create 2 nexuses, connect over NVMe, start fio, create and connect a 3rd nexus."""

    dev = create_nexus_dev
    dev2 = create_nexus_2_dev
    start_fio
    dev3 = create_nexus_3_dev
    assert dev == dev2, "should have one namespace"
    assert dev == dev3, "should have one namespace"

    desc = nvme_list_subsystems(dev)
    paths = desc["Subsystems"][0]["Paths"]
    assert len(paths) == 3, "should have 3 paths"

    # wait for fio to complete
    time.sleep(15)

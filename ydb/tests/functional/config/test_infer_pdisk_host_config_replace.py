# -*- coding: utf-8 -*-
import logging
import time

import pytest
import yaml
from hamcrest import assert_that

from ydb.core.protos import blobstorage_config_pb2
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
import ydb.public.api.protos.ydb_config_pb2 as config_pb
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.common.wait_for import retry_assertions
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.util import LogLevels

logger = logging.getLogger(__name__)

CONST_64_GB = 64 * 1024**3
CONST_10_GB = 10 * 1024**3
CONST_EXPLICIT_SLOT_COUNT = 18
CONST_EXPLICIT_SLOT_SIZE_IN_UNITS = 1


def infer_expected_slot_count(disk_size, unit_size, max_slots):
    slot_count = max(1, round(disk_size / unit_size))
    slot_size_in_units = 1
    while round(slot_count / slot_size_in_units) > max_slots:
        slot_size_in_units *= 2
    return round(slot_count / slot_size_in_units), slot_size_in_units


@pytest.fixture(scope="function")
def cluster():
    log_configs = {
        'BS_NODE': LogLevels.DEBUG,
        'BS_CONTROLLER': LogLevels.DEBUG,
    }

    configurator = KikimrConfigGenerator(
        erasure=Erasure.NONE,
        nodes=1,
        use_in_memory_pdisks=False,
        static_pdisk_size=CONST_64_GB,
        dynamic_pdisks=[{'disk_size': CONST_64_GB}],
        dynamic_pdisk_size=CONST_64_GB,
        use_config_store=True,
        metadata_section={
            "kind": "MainConfig",
            "version": 0,
            "cluster": "",
        },
        separate_node_configs=True,
        simple_config=True,
        use_self_management=True,
        extra_grpc_services=['config'],
        additional_log_configs=log_configs,
    )

    cluster = KiKiMR(configurator=configurator)
    cluster.start()

    yield cluster

    cluster.stop()


def fetch_full_config(cluster):
    resp = cluster.config_client.fetch_all_configs()
    assert_that(resp.operation.status == StatusIds.SUCCESS)
    result = config_pb.FetchConfigResult()
    resp.operation.result.Unpack(result)
    return yaml.safe_load(result.config[0].config)


def replace_full_config(cluster, full_config):
    resp = cluster.config_client.replace_config(yaml.dump(full_config))
    assert_that(
        resp.operation.status == StatusIds.SUCCESS,
        "ReplaceConfig failed: %s" % resp.operation.issues,
    )


def set_infer_settings(full_config):
    # Config v2 fetch does not include blob_storage_config by default.
    full_config["config"].setdefault("blob_storage_config", {})
    full_config["config"]["blob_storage_config"]["infer_pdisk_slot_count_settings"] = {
        "rot": {
            "prefer_inferred_settings_over_explicit": False,
            "unit_size": CONST_10_GB,
            "max_slots": 24,
        }
    }


def add_explicit_pdisk_config_to_all_drives(full_config):
    for host_config in full_config["config"]["host_configs"]:
        for drive in host_config.get("drive", []):
            drive["pdisk_config"] = {
                "expected_slot_count": CONST_EXPLICIT_SLOT_COUNT,
                "slot_size_in_units": CONST_EXPLICIT_SLOT_SIZE_IN_UNITS,
            }


def bump_config_version(full_config):
    full_config["metadata"]["version"] = full_config["metadata"].get("version", 0) + 1


def pdisk_by_path(cluster):
    base_config = cluster.client.query_base_config().BaseConfig
    return {pdisk.Path: pdisk for pdisk in base_config.PDisk}


def activate_all_pdisks(cluster):
    for path in pdisk_by_path(cluster):
        cluster.client.pdisk_set_all_active(pdisk_path=path)


class TestInferPDiskHostConfigReplace(object):
    def test_remove_explicit_pdisk_config_via_replace_applies_infer(self, cluster):
        inferred_slot_count, inferred_slot_size_in_units = infer_expected_slot_count(
            CONST_64_GB, CONST_10_GB, 24,
        )
        assert inferred_slot_count != CONST_EXPLICIT_SLOT_COUNT

        full_config = fetch_full_config(cluster)
        add_explicit_pdisk_config_to_all_drives(full_config)
        set_infer_settings(full_config)
        bump_config_version(full_config)
        replace_full_config(cluster, full_config)
        activate_all_pdisks(cluster)

        pdisk_paths = sorted(pdisk_by_path(cluster))
        assert len(pdisk_paths) == 2

        def check_explicit_on_all_pdisks():
            for path in pdisk_paths:
                pdisk = pdisk_by_path(cluster)[path]
                assert pdisk.DriveStatus == blobstorage_config_pb2.EDriveStatus.ACTIVE
                assert pdisk.PDiskConfig.ExpectedSlotCount == CONST_EXPLICIT_SLOT_COUNT
                assert pdisk.PDiskConfig.SlotSizeInUnits == CONST_EXPLICIT_SLOT_SIZE_IN_UNITS
                assert pdisk.PDiskMetrics.SlotCount == CONST_EXPLICIT_SLOT_COUNT
                assert pdisk.PDiskMetrics.SlotSizeInUnits == CONST_EXPLICIT_SLOT_SIZE_IN_UNITS
        retry_assertions(check_explicit_on_all_pdisks)

        full_config = fetch_full_config(cluster)
        host_config = full_config["config"]["host_configs"][0]
        drives = host_config["drive"]
        assert len(drives) == 2

        drive_with_explicit_config = drives[0]
        drive_to_clear = drives[1]
        assert "pdisk_config" in drive_with_explicit_config
        drive_to_clear_path = drive_to_clear["path"]
        drive_to_clear.pop("pdisk_config", None)

        set_infer_settings(full_config)
        bump_config_version(full_config)
        replace_full_config(cluster, full_config)
        activate_all_pdisks(cluster)

        def check_mixed_explicit_and_inferred():
            fetched = fetch_full_config(cluster)
            fetched_drives = {
                drive["path"]: drive
                for drive in fetched["config"]["host_configs"][0]["drive"]
            }
            assert "pdisk_config" in fetched_drives[drive_with_explicit_config["path"]]
            assert "pdisk_config" not in fetched_drives[drive_to_clear_path]

            for path in pdisk_paths:
                pdisk = pdisk_by_path(cluster)[path]
                assert pdisk.DriveStatus == blobstorage_config_pb2.EDriveStatus.ACTIVE
                if path == drive_to_clear_path:
                    assert pdisk.PDiskConfig.ExpectedSlotCount == inferred_slot_count, path
                    assert pdisk.PDiskConfig.SlotSizeInUnits == inferred_slot_size_in_units, path
                    assert pdisk.PDiskMetrics.SlotCount == inferred_slot_count, path
                    assert pdisk.PDiskMetrics.SlotSizeInUnits == inferred_slot_size_in_units, path
                else:
                    assert pdisk.PDiskConfig.ExpectedSlotCount == CONST_EXPLICIT_SLOT_COUNT, path
                    assert pdisk.PDiskConfig.SlotSizeInUnits == CONST_EXPLICIT_SLOT_SIZE_IN_UNITS, path
                    assert pdisk.PDiskMetrics.SlotCount == CONST_EXPLICIT_SLOT_COUNT, path
                    assert pdisk.PDiskMetrics.SlotSizeInUnits == CONST_EXPLICIT_SLOT_SIZE_IN_UNITS, path
        retry_assertions(check_mixed_explicit_and_inferred)

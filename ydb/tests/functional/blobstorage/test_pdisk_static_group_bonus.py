#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
import logging
import requests
import pytest
import json

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.util import LogLevels
from ydb.core.protos import msgbus_pb2, blobstorage_disk_pb2, whiteboard_disk_states_pb2

logger = logging.getLogger(__name__)

CONST_4_GB = 4 * 1024**3


class TestPDiskStaticGroupBonus(object):
    erasure = Erasure.BLOCK_4_2
    pool_name = 'dynamic_storage_pool:1'
    static_pdisk_id = 1
    dynamic_pdisk_id = 1000
    nodes_count = 8

    @pytest.fixture(autouse=True)
    def setup(self):
        log_configs = {
            'BS_NODE': LogLevels.DEBUG,
            'BS_PDISK': LogLevels.DEBUG,
        }

        self.configurator = KikimrConfigGenerator(
            erasure=self.erasure,
            nodes=self.nodes_count,
            use_in_memory_pdisks=False,
            static_pdisk_size=CONST_4_GB,
            dynamic_pdisks=[{'disk_size': CONST_4_GB}],
            dynamic_storage_pools=[dict(name=self.pool_name, kind="hdd", pdisk_user_kind=0)],
            additional_log_configs=log_configs
        )
        self.cluster = KiKiMR(configurator=self.configurator)
        self.cluster.start()

    def http_get(self, url):
        host = self.cluster.nodes[1].host
        port = self.cluster.nodes[1].mon_port
        return requests.get("http://%s:%s%s" % (host, port, url))

    def retriable(self, check_fn, timeout=30, delay=1):
        deadline = time.time() + timeout

        while True:
            try:
                return check_fn()
            except AssertionError as e:
                if time.time() > deadline:
                    raise e from e
                else:
                    time.sleep(delay)

    def get_pdisk_info(self, node_id, pdisk_id):
        response = self.http_get('/pdisk/info?node_id=%s&pdisk_id=%s' % (node_id, pdisk_id)).json()
        return response

    def test(self):
        pdisk_info = self.get_pdisk_info(1, self.static_pdisk_id)
        logger.info(json.dumps(pdisk_info, indent=2))
        time.sleep(1000)

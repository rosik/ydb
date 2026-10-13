PY3TEST()

SIZE(SMALL)

SRCDIR(ydb/apps/dstool)

PY_SRCS(main.py=ydb.apps.dstool.main)

TEST_SRCS(
    test_write2x2service.py
    test_capacity_metrics.py
    test_connection_tokens.py
    test_nbs_dbg_like_load.py
    test_cluster_balance.py
    test_cluster_reanimate.py
    test_cluster_workload_config.py
    test_cluster_workload_runtime.py
)

PEERDIR(
    contrib/python/PyYAML
    contrib/python/pytest
    ydb/apps/dstool/lib
)

END()

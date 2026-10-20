PY3_LIBRARY()

PY_SRCS(
    client.py
    connection.py
)

PEERDIR(
    contrib/python/rich
    contrib/python/textual
)

END()

RECURSE_FOR_TESTS(ut)

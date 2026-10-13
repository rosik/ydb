import io
import json
import os
from pathlib import Path
import socket
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from ydb.apps.dstool import main as cli
from ydb.apps.dstool.lib import write2x2_service as svc
from ydb.apps.dstool.lib import commands, common
from ydb.apps.dstool.lib import dstool_cmd_cluster_write2x2service as service_command
from ydb.apps.dstool.lib.arg_parser import ArgumentParser
from ydb.core.protos import blobstorage_config_pb2 as bsconfig


BLOB = '[42:7:258:3:5:4096:0]'


def layout(gid=1, nodes=None):
    nodes = nodes or list(range(1, 10))
    return dict(id=gid, generation=1, pool='static', fingerprint='v1',
                slots=[dict(index=i, node=node, pdisk=1, slot=100 + i) for i, node in enumerate(nodes)])


def args(**kwargs):
    values = dict(interval=3600, retry=60, refresh=300, lease=120, viewer='http://localhost:8765',
                  endpoint=['localhost'], request_timeout=5, scan_timeout=30, repair_delay=0,
                  master='unused', secret_file=None, poll=1, heartbeat=1)
    values.update(kwargs)
    return SimpleNamespace(**values)


class TestMaster(unittest.TestCase):
    def setUp(self):
        svc.STOP.clear()
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.master = svc.Master(args(), self.directory)
        self.addCleanup(lambda: self.master.db.close())
        self.add_group(layout())

    def add_group(self, item):
        with self.master.db:
            self.master.db.execute('INSERT INTO groups(id,layout,seen) VALUES(?,?,?)',
                                   (item['id'], json.dumps(item), time.time()))

    def claim(self, worker='one'):
        return self.master.rpc('/claim', {'worker': worker})['job']

    def result(self, job, findings, worker='one', completed=None):
        completed = list(range(9)) if completed is None else completed
        result = dict(findings=findings, completed_vdisks=completed,
                      missing_vdisks=sorted(set(range(9)) - set(completed)))
        return self.master.rpc('/result', dict(worker=worker, group=job['layout']['id'], token=job['token'], result=result))

    def test_retry_claim_node_exclusion_and_pause(self):
        self.add_group(layout(2))
        self.add_group(layout(3, list(range(11, 20))))
        job = self.claim()
        self.assertEqual(self.claim()['token'], job['token'])
        self.assertEqual(self.claim('two')['layout']['id'], 3)
        self.assertIsNone(self.claim('three'))
        self.master.console('pause')
        self.assertEqual(json.loads(self.master.console('stats'))['paused'], '1')

    def test_discovery_assigns_dynamic_group_from_protobuf(self):
        gid = 0x80000001
        base = bsconfig.TBaseConfig()
        group = base.Group.add(GroupId=gid, GroupGeneration=7, ErasureSpecies='mirror-3-dc',
                               BoxId=1, StoragePoolId=2)
        for index in range(9):
            slot_id = group.VSlotId.add(NodeId=11 + index, PDiskId=1, VSlotId=100 + index)
            slot = base.VSlot.add(GroupId=gid, GroupGeneration=7, FailRealmIdx=index // 3,
                                  FailDomainIdx=index % 3, VDiskIdx=0)
            slot.VSlotId.CopyFrom(slot_id)
        base.Group.add(GroupId=gid + 1, ErasureSpecies='block-4-2')
        virtual = base.Group.add(GroupId=gid + 2, ErasureSpecies='mirror-3-dc')
        virtual.VirtualGroupInfo.SetInParent()
        pool = bsconfig.TDefineStoragePool(BoxId=1, StoragePoolId=2, Name='dynamic-pool')
        data = dict(BaseConfig=base, StoragePools=[pool])
        with patch.object(common, 'fetch_base_config_and_storage_pools', return_value=data), \
                patch.object(svc, 'view', return_value={}), \
                patch.object(self.master.refresh, 'wait', side_effect=lambda *_: svc.STOP.set()):
            try:
                self.master.discover()
            finally:
                svc.STOP.clear()
        self.assertEqual(json.loads(self.master.console('stats'))['discovery_error'], '')
        self.assertEqual(json.loads(self.master.console('stats'))['groups'], 2)
        self.claim('static-worker')
        discovered = self.claim('dynamic-worker')['layout']
        self.assertEqual(discovered['id'], gid)
        self.assertEqual(discovered['generation'], 7)
        self.assertEqual(discovered['pool'], 'dynamic-pool')
        self.assertEqual([s['index'] for s in discovered['slots']], list(range(9)))
        self.assertEqual([s['node'] for s in discovered['slots']], list(range(11, 20)))

    def test_expired_lease_rejects_old_results(self):
        old = self.claim()
        with self.master.db:
            self.master.db.execute('UPDATE groups SET expires=0')
        new = self.claim('two')
        self.assertFalse(self.result(old, {BLOB: 'request_ok'})['accepted'])
        self.assertTrue(self.result(new, {}, 'two')['accepted'])

    def test_result_idempotence_reappearance_and_restart(self):
        job = self.claim()
        self.assertTrue(self.result(job, {BLOB: 'request_ok'})['accepted'])
        self.assertTrue(self.result(job, {BLOB: 'request_ok'})['accepted'])
        self.assertEqual(json.loads(self.master.console('stats'))['scans'], 1)
        self.master.db.close()
        self.master = svc.Master(args(), self.directory)
        self.assertEqual(json.loads(self.master.console('stats'))['unresolved_findings'], 1)
        self.master.console('scan all')
        self.result(self.claim(), {})
        self.assertEqual(json.loads(self.master.console('stats'))['unresolved_findings'], 0)
        self.master.console('scan 1')
        self.result(self.claim(), {BLOB: 'absent'})
        self.assertEqual(json.loads(self.master.console('stats'))['new_total'], 2)

    def test_failed_scan_preserves_findings(self):
        self.result(self.claim(), {BLOB: 'request_ok'})
        self.master.console('scan all')
        job = self.claim()
        self.master.rpc('/result', dict(worker='one', group=1, token=job['token'], result={'error': 'stream lost'}))
        self.assertEqual(json.loads(self.master.console('stats'))['unresolved_findings'], 1)

    def test_coverage_change_does_not_resolve_findings(self):
        self.result(self.claim(), {BLOB: 'request_ok'}, completed=list(range(6)))
        self.master.console('scan all')
        self.result(self.claim(), {}, completed=list(range(3, 9)))
        self.assertEqual(json.loads(self.master.console('stats'))['unresolved_findings'], 1)
        self.master.console('scan all')
        self.result(self.claim(), {})
        self.assertEqual(json.loads(self.master.console('stats'))['unresolved_findings'], 0)

    def test_repeated_changed_coverage_preserves_findings_after_restart(self):
        self.result(self.claim(), {BLOB: 'request_ok'}, completed=list(range(6)))
        self.master.console('scan all')
        self.result(self.claim(), {}, completed=list(range(3, 9)))
        self.master.db.close()
        self.master = svc.Master(args(), self.directory)
        self.master.console('scan all')
        self.result(self.claim(), {}, completed=list(range(3, 9)))
        self.assertEqual(json.loads(self.master.console('stats'))['unresolved_findings'], 1)
        row = self.master.db.execute('SELECT due,last_result FROM groups').fetchone()
        self.assertAlmostEqual(row['due'] - time.time(), args().retry, delta=5)
        self.assertEqual(json.loads(row['last_result'])['disappeared'], 0)
        self.assertEqual(json.loads(row['last_result'])['unresolved'], 1)
        self.master.console('scan all')
        self.result(self.claim(), {}, completed=list(range(6)))
        self.assertEqual(json.loads(self.master.console('stats'))['unresolved_findings'], 0)

    def test_findings_resolved_only_against_their_own_coverage(self):
        other_blob = '[42:7:259:3:5:4096:0]'
        self.result(self.claim(), {BLOB: 'request_ok'}, completed=list(range(6)))
        self.master.console('scan all')
        self.result(self.claim(), {other_blob: 'absent'}, completed=list(range(3, 9)))
        self.master.console('scan all')
        self.result(self.claim(), {}, completed=list(range(3, 9)))
        active = [row[0] for row in self.master.db.execute('SELECT blob FROM findings WHERE active=1')]
        self.assertEqual(active, [BLOB])
        result = json.loads(self.master.db.execute('SELECT last_result FROM groups').fetchone()[0])
        self.assertEqual(result['disappeared'], 1)

    def test_invalid_coverage_rolls_back(self):
        job = self.claim()
        with self.assertRaises(ValueError):
            self.result(job, {}, completed=[0, 1, 2])
        self.assertEqual(json.loads(self.master.console('stats'))['scans'], 0)

    def test_http_transport_and_auth(self):
        server = svc.http_server.ThreadingHTTPServer(('127.0.0.1', 0), svc.API)
        server.master, server.api_secret = self.master, 'secret'
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        try:
            url = 'http://127.0.0.1:%d/claim' % server.server_port
            with self.assertRaises(svc.urllib.error.HTTPError):
                svc.http(url, data={'worker': 'one'})
            result = svc.http(url, {'Authorization': 'Bearer secret'}, {'worker': 'one'})
            self.assertEqual(result['job']['layout']['id'], 1)
        finally:
            server.shutdown()
            thread.join()
            server.server_close()

    def test_console_socket(self):
        path = str(self.directory / 'console.sock')
        server = svc.UnixServer(path, svc.Console)
        server.master = self.master
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        try:
            with socket.socket(socket.AF_UNIX) as conn:
                conn.settimeout(3)
                conn.connect(path)
                self.assertIn(b'groups', conn.recv(8192))
                conn.sendall(b'pause\n')
                self.assertIn(b'OK', conn.recv(8192))
                conn.sendall(b'quit\n')
            self.assertEqual(self.master.setting('paused'), 1)
        finally:
            server.shutdown()
            thread.join()
            server.server_close()

    def test_worker_repairs_then_verifies_over_http(self):
        repaired, reports = False, []
        sequences = {}

        def ydb(_args, url, raw=False):
            nonlocal repaired
            parsed = svc.urllib.parse.urlsplit(url)
            if parsed.path.endswith('bsgroupinfo'):
                return {'BSGroupStateInfo': [{'GroupID': 1, 'GroupGeneration': 1, 'ErasureSpecies': 'mirror-3-dc'}]}
            if parsed.path.endswith('tenantinfo'):
                return {'TenantInfo': []}
            if parsed.path.endswith('sysinfo'):
                return {'SystemStateInfo': [{'NodeId': 1}]}
            if parsed.path.endswith('blob_range'):
                repaired = True
                return {'Status': 'OK', 'Blobs': [BLOB]}
            params = svc.urllib.parse.parse_qs(parsed.query)
            session = params['sessionId'][0]
            if session in sequences:
                return b''
            sequences[session] = True
            node = int(parsed.path.split('/')[2])
            return b'opaque snapshot bytes for node %d' % node

        original = self.master.rpc

        def rpc(action, body):
            result = original(action, body)
            if action == '/result':
                reports.append(body['result'])
                if len(reports) == 1:
                    self.master.console('scan all')
                else:
                    svc.STOP.set()
            return result

        server = svc.http_server.ThreadingHTTPServer(('127.0.0.1', 0), svc.API)
        server.master, server.api_secret = self.master, ''
        server_thread = threading.Thread(target=server.serve_forever)
        server_thread.start()
        directory = self.directory / 'worker'
        directory.mkdir()
        timer = threading.Timer(10, svc.STOP.set)
        timer.start()
        try:
            with patch.object(svc, 'ydb_http', side_effect=ydb), patch.object(self.master, 'rpc', side_effect=rpc), \
                    patch.object(svc, 'run_blobsan', side_effect=lambda *_: set() if repaired else {BLOB}):
                svc.work(args(master='127.0.0.1:%d' % server.server_port), directory)
            self.assertEqual(len(reports), 2)
            self.assertEqual(reports[0]['findings'], {BLOB: 'request_ok'})
            self.assertEqual(reports[1]['findings'], {})
            self.assertEqual(json.loads(self.master.console('stats'))['unresolved_findings'], 0)
        finally:
            timer.cancel()
            server.shutdown()
            server_thread.join()
            server.server_close()
            svc.STOP.clear()


class TestWorker(unittest.TestCase):
    def setUp(self):
        svc.STOP.clear()
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)

    def test_relative_blobsan_runs_checked_executable(self):
        binary = self.directory / 'blobsan'
        binary.write_text('#!/bin/sh\n'
                          '[ "$1" = mirror-3-dc ] || exit 2\n'
                          'IFS= read -r snapshot\n'
                          '[ "$snapshot" = "opaque snapshot bytes" ] || exit 3\n'
                          "printf '%s\\n' 'GroupId# 1 BlobId# " + BLOB + "' >&2\n")
        binary.chmod(0o700)
        (self.directory / 'snapshot').write_bytes(b'opaque snapshot bytes\n')
        decoy_dir = self.directory / 'bin'
        decoy_dir.mkdir()
        decoy = decoy_dir / 'blobsan'
        decoy.write_text('#!/bin/sh\nexit 99\n')
        decoy.chmod(0o700)

        def work(parsed, directory):
            self.assertEqual(parsed.blobsan, str(binary.resolve()))
            self.assertEqual(svc.run_blobsan(parsed, 1, self.directory, threading.Event()), {BLOB})

        previous_cwd = Path.cwd()
        try:
            os.chdir(self.directory)
            with patch.dict(os.environ, {'PATH': str(decoy_dir)}), \
                    patch.object(common, 'connection_params', common.ConnectionParams()), \
                    patch.object(svc, 'work', side_effect=work) as worker:
                service_command.do(args(write2x2_action='worker', dry_run=False,
                                        blobsan='./blobsan', db=str(self.directory / 'db')))
                worker.assert_called_once()
        finally:
            os.chdir(previous_cwd)

    def test_scan_six_streams_and_reject_partial_second_realm(self):
        def run(failed):
            counts = {}

            def fetch(_args, url, raw=False):
                parsed = svc.urllib.parse.urlsplit(url)
                node = int(parsed.path.split('/')[2])
                counts[node] = counts.get(node, 0) + 1
                if node in failed:
                    raise OSError('unavailable')
                if counts[node] == 1:
                    return b'opaque snapshot bytes'
                return b''

            with patch.object(svc, 'ydb_http', side_effect=fetch), patch.object(svc, 'run_blobsan', return_value={BLOB}):
                return svc.scan(args(), {'layout': layout()}, self.directory, threading.Event())

        found, coverage = run({7, 8, 9})
        self.assertEqual(found, {BLOB})
        self.assertEqual(coverage['missing_vdisks'], [6, 7, 8])
        with self.assertRaises(RuntimeError):
            run({6, 7, 8, 9})

    def test_repair_json_semantics_and_expired_local_lease(self):
        responses = [{'TenantInfo': []}, {'SystemStateInfo': [{'NodeId': 10}]}, {'Status': 'OK', 'Blobs': []}]
        with patch.object(svc, 'ydb_http', side_effect=responses):
            self.assertEqual(svc.repair(args(), {'layout': layout()}, {BLOB}, threading.Event()), {BLOB: 'absent'})
        cancel = threading.Event()
        cancel.deadline = time.monotonic() - 1
        with self.assertRaises(RuntimeError):
            svc.check_running(cancel)

    def test_pending_result_replayed_after_restart(self):
        svc.atomic_json(self.directory / 'worker.json', {'id': 'worker'})
        svc.atomic_json(self.directory / 'pending.json', {'group': 1, 'token': 'lease', 'result': {'error': 'test'}})

        def reply(url, headers, data, **kwargs):
            self.assertTrue(url.endswith('/result'))
            self.assertEqual(data['token'], 'lease')
            svc.STOP.set()
            return {'accepted': True}

        try:
            with patch.object(svc, 'http', side_effect=reply):
                svc.work(args(), self.directory)
            self.assertFalse((self.directory / 'pending.json').exists())
        finally:
            svc.STOP.clear()

    def test_scratch_failure_reports_error_and_stops_heartbeat(self):
        (self.directory / 'scan').write_text('not a directory')
        replies = []

        def reply(url, headers, data, **kwargs):
            if url.endswith('/claim'):
                return {'job': dict(layout=layout(), token='lease', lease_seconds=120)}
            self.assertTrue(url.endswith('/result'))
            replies.append(data)
            svc.STOP.set()
            return {'accepted': True}

        with patch.object(svc, 'http', side_effect=reply):
            svc.work(args(), self.directory)
        self.assertEqual(len(replies), 1)
        self.assertIn('error', replies[0]['result'])
        svc.STOP.clear()


class TestCLI(unittest.TestCase):
    def setUp(self):
        version = patch.object(cli, 'get_version', return_value='test')
        version.start()
        self.addCleanup(version.stop)

    def test_service_endpoint_default_and_explicit_spellings(self):
        for action, tail in [('master', ['--console-sock', '/tmp/socket']),
                             ('worker', ['--master', 'host:22570'])]:
            command = ['cluster', 'write2x2service', action, '--db', '/tmp/db'] + tail
            for endpoints, expected in [([], ['localhost']),
                                        (['-eremote.example'], ['remote.example']),
                                        (['-e', 'remote.example'], ['remote.example']),
                                        (['--endpoint', 'remote.example'], ['remote.example']),
                                        (['--endpoint=remote.example'], ['remote.example']),
                                        (['-efirst.example', '-esecond.example'], ['first.example', 'second.example'])]:
                argv = endpoints + command
                with self.subTest(argv=argv), \
                        patch.object(common, 'connection_params', common.ConnectionParams()), \
                        patch.object(common, 'apply_args'), patch.object(commands, 'run_command') as run:
                    cli.main(argv)
                    parsed = run.call_args.args[1]
                    self.assertEqual(parsed.endpoint, expected)
                    self.assertEqual(svc.viewer_url(parsed), 'http://' + expected[0] + ':8765')

    def test_other_commands_still_require_endpoint(self):
        with patch.object(common, 'apply_args') as apply_args, \
                patch.object(commands, 'run_command') as run, \
                patch('sys.stderr', new_callable=io.StringIO), patch('sys.stdout', new_callable=io.StringIO), \
                self.assertRaises(SystemExit) as error:
            cli.main(['cluster', 'list'])
        self.assertEqual(error.exception.code, 1)
        apply_args.assert_not_called()
        run.assert_not_called()

    def test_commands_parse(self):
        for tail in (['master', '-l', '22570', '--db', '/tmp/master', '--console-sock', '/tmp/socket'],
                     ['worker', '--master', 'host:22570', '--db', '/tmp/worker']):
            parser = ArgumentParser()
            commands.make_command_map_by_structure(parser.add_subparsers(dest='global_command', required=True))
            parsed = parser.parse_args(['cluster', 'write2x2service'] + tail)
            self.assertEqual(parsed.write2x2_action, tail[0])

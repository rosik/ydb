import asyncio
import json
from pathlib import Path
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from textual.widgets import Button, DataTable, Input, RichLog, Static, TabbedContent

from ydb.apps.dstool.lib import dstool_cmd_cluster_write2x2service as command
from ydb.apps.dstool.lib import common, write2x2_report
from ydb.apps.dstool.lib import write2x2_service as svc
from ydb.apps.dstool.tui.client import ConsoleApp
from ydb.apps.dstool.tui.connection import ConsoleConnection


class RecordingConnection(ConsoleConnection):
    def __init__(self, path):
        super().__init__(path, timeout=2)
        self.commands = []

    async def request(self, command):
        self.commands.append(command)
        return await super().request(command)


class ConsoleAppTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        svc.STOP.clear()
        self.temp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.temp.name) / 'console.sock')
        self.master = svc.Master(SimpleNamespace(interval=3600, retry=60, refresh=300, lease=120), Path(self.temp.name))
        self.gid = 2147483649
        now = time.time()
        for gid in (self.gid, self.gid + 1):
            layout = dict(id=gid, generation=7, pool='pool [test]', pool_key='1:7', fingerprint='v1', slots=[])
            with self.master.db:
                self.master.db.execute('INSERT INTO groups(id,layout,seen) VALUES(?,?,?)', (gid, json.dumps(layout), now))
        job = self.master.rpc('/claim', dict(worker='worker-1', info='host1'))['job']
        bucket = dict(local='1+1+?', ingress='1+1+1', gc='no_covering_barrier', blobs=4,
                      logical_bytes=4096, observed_replica_bytes=8192, missing_metadata_blobs=0)
        report = dict(version=1, group_id=self.gid, completed_vdisks=list(range(6)),
                      buckets=[bucket], totals=write2x2_report.totals([bucket]))
        self.master.rpc('/result', dict(worker='worker-1', group=self.gid, token=job['token'], result=dict(
            findings={'[42:7:258:3:5:4096:0]': 'request_ok'}, completed_vdisks=list(range(6)),
            missing_vdisks=[6, 7, 8], scan_report=report, started_at=now - 2, finished_at=now - 1)))
        self.server = svc.UnixServer(self.path, svc.Console)
        self.server.master = self.master
        self.thread = threading.Thread(target=lambda: self.server.serve_forever(poll_interval=0.01), daemon=True)
        self.thread.start()
        self.connection = RecordingConnection(self.path)

    async def asyncTearDown(self):
        await self.connection.close()
        await asyncio.to_thread(self.server.shutdown)
        self.server.server_close()
        self.thread.join()
        self.master.db.close()
        self.temp.cleanup()

    def app(self, refresh=60):
        return ConsoleApp(self.path, refresh=refresh, connection=self.connection)

    async def settle(self, app, pilot):
        for _ in range(100):
            await pilot.pause(0.01)
            if not app._refreshing and not app._command_busy:
                return
        self.fail('UI did not finish refreshing')

    async def tab(self, app, pilot, name):
        app.query_one('#views', TabbedContent).active = name
        await self.settle(app, pilot)

    async def test_dashboard_drilldown_and_navigation_against_master(self):
        app = self.app()
        async with app.run_test(size=(120, 35)) as pilot:
            await self.settle(app, pilot)
            pool_table = app.query_one('#pool-table', DataTable)
            self.assertEqual([str(c) for c in pool_table.get_row_at(0)][:5], ['pool [test]', '1:7', '1/2', '0', '1'])
            pool_table.focus()
            await pilot.press('enter')
            await self.settle(app, pilot)
            self.assertEqual(app.detail_target, ('pool', '1:7'))
            layout = app.query_one('#layout-table', DataTable)
            self.assertEqual([str(c) for c in layout.get_row_at(0)][:4], ['1+1+?', '1+1+1', 'no_covering_barrier', '4'])
            await self.tab(app, pilot, 'groups')
            groups = app.query_one('#group-table', DataTable)
            self.assertEqual(groups.row_count, 2)
            groups.focus()
            await pilot.press('enter')
            await self.settle(app, pilot)
            self.assertEqual(app.detail_target, ('group', str(self.gid)))
            self.assertEqual(layout.row_count, 1)
            self.assertIn('missing indexes: 6,7,8', str(app.query_one('#detail', Static).content))
            await self.tab(app, pilot, 'workers')
            self.assertEqual(str(app.query_one('#worker-table', DataTable).get_row_at(0)[0]), 'worker-1')
            await self.tab(app, pilot, 'findings')
            findings = app.query_one('#finding-table', DataTable)
            self.assertEqual(findings.row_count, 1)
            findings.focus()
            await pilot.press('enter')
            await self.settle(app, pilot)
            self.assertEqual(app.detail_target, ('group', str(self.gid)))
            await pilot.press(']')
            await self.settle(app, pilot)
            self.assertEqual(app.query_one('#views', TabbedContent).active, 'workers')
            await pilot.press('[')
            await self.settle(app, pilot)
            self.assertEqual(app.query_one('#views', TabbedContent).active, 'layouts')
            self.assertTrue(all(c.split()[0] in ('stats', 'pools', 'groups', 'layouts', 'workers', 'findings')
                                for c in self.connection.commands))
        self.assertIsNone(self.connection._writer)

    async def test_buttons_and_raw_commands(self):
        app = self.app()
        async with app.run_test(size=(100, 30)) as pilot:
            await self.settle(app, pilot)
            for state in ('1', '0'):
                await pilot.click('#pause')
                await self.settle(app, pilot)
                self.assertEqual(json.loads(self.master.console('stats'))['paused'], state)
            await pilot.click('#scan')
            await self.settle(app, pilot)
            self.assertEqual(self.master.db.execute('SELECT max(due) FROM groups').fetchone()[0], 0)
            await pilot.click('#inventory')
            await self.settle(app, pilot)
            self.assertTrue(self.master.refresh.is_set())
            await pilot.press('ctrl+l')
            field = app.query_one('#command', Input)
            self.assertTrue(field.has_focus)
            field.value = 'interval 12'
            await pilot.press('enter')
            await self.settle(app, pilot)
            self.assertEqual(self.master.setting('interval'), 12)
            field.value = 'bad-command'
            await pilot.press('enter')
            await self.settle(app, pilot)
            log = app.query_one('#log', RichLog)
            self.assertIn('ERROR: Unknown command', '\n'.join(line.text for line in log.lines))
            field.value = 'quit'
            await pilot.press('enter')
        self.assertEqual(self.connection.commands.count('pause'), 1)
        self.assertEqual(self.connection.commands.count('resume'), 1)
        self.assertNotIn('quit', self.connection.commands)

    async def test_disconnection_marks_retained_data_stale_and_recovers(self):
        app = self.app()
        async with app.run_test(size=(80, 24)) as pilot:
            await self.settle(app, pilot)
            await self.connection.close()
            self.connection.path = str(Path(self.temp.name) / 'missing.sock')
            app.action_refresh()
            await self.settle(app, pilot)
            self.assertFalse(app._connected)
            self.assertEqual(app.query_one('#pool-table', DataTable).row_count, 1)
            self.assertIn('stale', str(app.query_one('#status', Static).content))
            self.assertTrue(app.query_one('#scan', Button).disabled)
            self.connection.path = self.path
            await pilot.press('f5')
            await self.settle(app, pilot)
            self.assertTrue(app._connected)
            self.assertFalse(app.query_one('#scan', Button).disabled)

    async def test_periodic_refresh_preserves_group_selection(self):
        app = self.app(refresh=0.1)
        async with app.run_test(size=(100, 30)) as pilot:
            await self.settle(app, pilot)
            await self.tab(app, pilot, 'groups')
            table = app.query_one('#group-table', DataTable)
            table.move_cursor(row=1)
            self.master.console('pause')
            for _ in range(100):
                await pilot.pause(0.02)
                if app.stats.get('paused') == '1':
                    break
            self.assertEqual(app.stats['paused'], '1')
            self.assertEqual(table.cursor_row, 1)
            self.assertGreaterEqual(self.connection.commands.count('groups brief'), 2)
            table.focus()
            await pilot.press('enter')
            await self.settle(app, pilot)
            self.assertEqual(app.query_one('#layout-table', DataTable).row_count, 0)
            self.assertIn('No successful scan report', str(app.query_one('#detail', Static).content))


class ClientCommandTest(unittest.TestCase):
    def test_dispatch_needs_no_service_database_or_cluster_connection(self):
        args = SimpleNamespace(write2x2_action='client', console_sock='/tmp/test.sock', refresh=3,
                               request_timeout=7, dry_run=False)
        with patch('ydb.apps.dstool.tui.client.ConsoleApp') as app:
            command.do(args)
        app.assert_called_once_with('/tmp/test.sock', refresh=3, timeout=7)
        app.return_value.run.assert_called_once_with()

    def test_dry_run_rejected_before_starting_client(self):
        with patch('ydb.apps.dstool.tui.client.ConsoleApp') as app, \
                self.assertRaises(common.InvalidParameterError):
            command.do(SimpleNamespace(write2x2_action='client', dry_run=True))
        app.assert_not_called()

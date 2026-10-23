import asyncio
import json
from pathlib import Path
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from textual.widgets import Button, Checkbox, Collapsible, DataTable, Input, RichLog, Select, Static, TabbedContent

from ydb.apps.dstool.lib import dstool_cmd_blobsan_service as command
from ydb.apps.dstool.lib import common, write2x2_report
from ydb.apps.dstool.lib import blobsan_service as svc
from ydb.apps.dstool.tui.client import ConsoleApp, size
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
                      logical_bytes=4096, observed_replica_bytes=8192, missing_metadata_blobs=0,
                      phantom_like=False, inv1_candidate=False, inv2_candidate=True)
        report = dict(version=1, group_id=self.gid, completed_vdisks=list(range(6)),
                      buckets=[bucket], totals=write2x2_report.totals([bucket]))
        self.master.rpc('/result', dict(worker='worker-1', group=self.gid, token=job['token'], result=dict(
            candidates={'inv1_candidate': [],
                        'inv2_candidate': ['[42:7:258:3:5:4096:0]']},
            candidates_complete=True, completed_vdisks=list(range(6)),
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
        await pilot.click(app.query_one('#views', TabbedContent).get_tab(name))
        await self.settle(app, pilot)

    def replace_report(self, buckets, completed=tuple(range(6)), gid=None):
        gid = self.gid if gid is None else gid
        self.master.console('scan %d' % gid)
        job = self.master.rpc('/claim', dict(worker='worker-1'))['job']
        self.assertEqual(job['layout']['id'], gid)
        now = time.time()
        report = dict(version=1, group_id=gid, completed_vdisks=list(completed),
                      buckets=buckets, totals=write2x2_report.totals(buckets))
        self.master.rpc('/result', dict(worker='worker-1', group=gid, token=job['token'], result=dict(
            candidates={'inv1_candidate': [], 'inv2_candidate': []}, candidates_complete=False, completed_vdisks=list(completed),
            missing_vdisks=sorted(set(range(9)) - set(completed)),
            scan_report=report, started_at=now - 1, finished_at=now)))

    async def open_pool(self, app, pilot):
        await self.settle(app, pilot)
        app.query_one('#pool-table', DataTable).focus()
        await pilot.press('enter')
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
            self.assertEqual([str(c) for c in layout.get_row_at(0)], ['1+1', '4', '4.0 KiB', 'no', 'no', 'yes'])
            await self.tab(app, pilot, 'groups')
            groups = app.query_one('#group-table', DataTable)
            self.assertEqual(groups.row_count, 2, str(app.query_one('#status', Static).content))
            groups.focus()
            await pilot.press('enter')
            await self.settle(app, pilot)
            self.assertEqual(app.detail_target, ('group', str(self.gid)))
            self.assertEqual(layout.row_count, 1)
            self.assertIn('missing indexes: 6,7,8', str(app.query_one('#detail', Static).content))
            await self.tab(app, pilot, 'workers')
            self.assertEqual(str(app.query_one('#worker-table', DataTable).get_row_at(0)[0]), 'worker-1')
            await self.tab(app, pilot, 'candidates')
            findings = app.query_one('#candidate-table', DataTable)
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
            self.assertTrue(all(c.split()[0] in ('stats', 'pools', 'groups', 'layouts', 'workers', 'candidates')
                                for c in self.connection.commands))
        self.assertIsNone(self.connection._writer)

    async def test_layout_groups_ingress_and_gc_and_exposes_filtered_details(self):
        def bucket(blobs, gc='no_covering_barrier', phantom=False, candidate=False, **kwargs):
            result = dict(local='1+1+?', ingress='1+1+1', gc=gc, blobs=blobs,
                          logical_bytes=1024 * blobs, observed_replica_bytes=2048 * blobs,
                          missing_metadata_blobs=blobs if phantom else 0,
                          phantom_like=phantom, inv1_candidate=False, inv2_candidate=candidate)
            return dict(result, **kwargs)

        buckets = [bucket(3, candidate=True), bucket(2, gc='soft_with_keep', phantom=True),
                   bucket(1, ingress='2+2+2')]
        # A different ingress with the same Local belongs to the same summary row.
        buckets += [bucket(5, gc='hard_barrier', phantom=True), bucket(7, gc='soft_without_keep'), bucket(11, gc='do_not_keep')]
        large = bucket(10)
        large['local'] = '2+2+?'
        buckets.append(large)
        self.replace_report(buckets)
        app = self.app()
        async with app.run_test(size=(150, 50)) as pilot:
            await self.open_pool(app, pilot)
            table = app.query_one('#layout-table', DataTable)
            self.assertEqual(table.row_count, 2)
            self.assertEqual([str(c) for c in table.get_row_at(0)],
                             ['1+1', '6', '6.0 KiB', 'mixed (2/6)', 'no', 'mixed (3/6)'])
            self.assertEqual([str(c) for c in table.get_row_at(1)], ['2+2', '10', '10.0 KiB', 'no', 'no', 'no'])
            summary = app.query_one('#gc-summary', Collapsible)
            self.assertTrue(summary.collapsed)
            self.assertIn('23 blobs', summary.title)
            self.assertEqual(app.query_one('#gc-table', DataTable).row_count, 3)
            table.move_cursor(row=0)
            table.focus()
            await pilot.press('enter')
            await pilot.pause()
            self.assertFalse(app.query_one('#bucket-details', Collapsible).collapsed)
            details = app.query_one('#bucket-table', DataTable)
            self.assertEqual(details.row_count, 3)
            self.assertEqual([str(c) for c in details.get_row_at(0)],
                             ['1+1+?', '1+1+?', '1+1+1', 'no_covering_barrier', '3',
                              '3.0 KiB', '6.0 KiB', '0', 'no', 'no', 'yes'])
            self.assertNotIn('Excluded:', str(app.query_one('#detail', Static).content))
            app.query_one('#layout-filter', Select).value = 'soft_with_keep'
            await pilot.pause()
            self.assertEqual([str(c) for c in table.get_row_at(0)], ['1+1', '2', '2.0 KiB', 'yes', 'no', 'no'])
            app.query_one('#layout-filter', Select).value = 'all'
            await pilot.pause()
            self.assertEqual([str(c) for c in table.get_row_at(0)][:3], ['1+1', '29', '29.0 KiB'])
            self.assertEqual(details.row_count, 6)
            await pilot.click('#show-phantom-like')
            await pilot.pause()
            self.assertEqual([str(c) for c in table.get_row_at(0)],
                             ['1+1', '22', '22.0 KiB', 'no', 'no', 'mixed (3/22)'])
            self.assertEqual(details.row_count, 4)
            self.assertIn('18 blobs', summary.title)
            self.assertEqual(app.query_one('#gc-table', DataTable).row_count, 2)
            self.assertIn('Selected: 32 blobs', str(app.query_one('#detail', Static).content))
            await pilot.click('#inv2-candidate-only')
            await pilot.pause()
            self.assertEqual(table.row_count, 1)
            self.assertEqual([str(c) for c in table.get_row_at(0)], ['1+1', '3', '3.0 KiB', 'no', 'no', 'yes'])
            self.assertEqual(details.row_count, 1)
            self.assertFalse(summary.display)
            app.query_one('#layout-filter', Select).value = 'hard_barrier'
            await pilot.pause()
            self.assertEqual(table.row_count, 0)
            self.assertEqual(details.row_count, 0)
            app.query_one('#layout-filter', Select).value = 'active'
            app.action_refresh()
            await self.settle(app, pilot)
            self.assertEqual(str(table.get_row_at(0)[1]), '3')
            self.assertFalse(app.query_one('#show-phantom-like', Checkbox).value)
            self.assertTrue(app.query_one('#inv2-candidate-only', Checkbox).value)
            await pilot.click('#inv2-candidate-only')
            await pilot.pause()
            self.assertEqual(str(table.get_row_at(0)[1]), '4')
            await pilot.click('#show-phantom-like')
            await pilot.pause()
            self.assertEqual(str(table.get_row_at(0)[1]), '6')

    async def test_pool_refresh_replaces_previous_scan_instead_of_adding_to_it(self):
        app = self.app()
        async with app.run_test(size=(120, 40)) as pilot:
            await self.open_pool(app, pilot)
            table = app.query_one('#layout-table', DataTable)
            self.assertEqual(str(table.get_row_at(0)[1]), '4')
            legacy = dict(local='2+2+?', ingress='2+2+2', gc='soft_with_keep', blobs=1,
                          logical_bytes=2**30, observed_replica_bytes=4 * 2**30, missing_metadata_blobs=0)
            self.replace_report([legacy])
            for _ in range(2):
                app.action_refresh()
                await self.settle(app, pilot)
                self.assertEqual(table.row_count, 1)
                self.assertEqual([str(c) for c in table.get_row_at(0)], ['2+2', '1', '1.0 GiB', 'unknown', 'unknown', 'unknown'])
            await pilot.click('#show-phantom-like')
            await pilot.pause()
            self.assertEqual(table.row_count, 1)
            await pilot.click('#inv2-candidate-only')
            await pilot.pause()
            self.assertEqual(table.row_count, 0)
            await pilot.click('#inv2-candidate-only')
            await pilot.pause()
            self.assertEqual(table.row_count, 1)

    async def test_local2_merges_permutations_and_third_realm_and_sorts_ascending(self):
        def bucket(local, blobs, phantom=False, candidate=False):
            return dict(local=local, ingress='3+3+3', gc='soft_with_keep', blobs=blobs,
                        logical_bytes=100 * blobs, observed_replica_bytes=200 * blobs,
                        missing_metadata_blobs=blobs if phantom else 0,
                        phantom_like=phantom, inv1_candidate=candidate, inv2_candidate=candidate)

        self.replace_report([
            bucket('2+2+2', 100), bucket('2+1+1', 3), bucket('1+2+1', 4, phantom=True),
            bucket('1+1+2', 5), bucket('2+1+0', 6, candidate=True), bucket('0+0+0', 1, phantom=True),
            bucket('3+1+1', 2), bucket('1+0+0', 30, phantom=True),
        ], completed=range(9))
        self.replace_report([bucket('2+2+?', 7), bucket('1+0+?', 8, phantom=True)], gid=self.gid + 1)
        app = self.app()
        async with app.run_test(size=(150, 50)) as pilot:
            await self.open_pool(app, pilot)
            table = app.query_one('#layout-table', DataTable)
            self.assertEqual([str(row[0]) for row in (table.get_row_at(i) for i in range(table.row_count))],
                             ['0+0', '1+0', '2+1', '2+2', '3+1'])
            self.assertEqual([str(table.get_row_at(i)[1]) for i in range(table.row_count)],
                             ['1', '38', '18', '107', '2'])
            self.assertEqual([str(cell) for cell in table.get_row_at(2)][3:], ['mixed (4/18)', 'mixed (6/18)', 'mixed (6/18)'])
            table.move_cursor(row=2)
            table.focus()
            await pilot.press('enter')
            await pilot.pause()
            details = app.query_one('#bucket-table', DataTable)
            self.assertEqual(details.row_count, 4)
            self.assertEqual([str(details.get_row_at(i)[0]) for i in range(details.row_count)],
                             ['2+1+0', '1+1+2', '1+2+1', '2+1+1'])
            self.assertEqual([str(details.get_row_at(i)[1]) for i in range(details.row_count)],
                             ['2+1+0', '2+1+1', '2+1+1', '2+1+1'])

    async def test_candidate_categories_group_scope_and_pagination(self):
        self.master.console('scan %d' % self.gid)
        job = self.master.rpc('/claim', dict(worker='worker-1'))['job']
        now = time.time()
        bucket = dict(local='0+1+?', ingress='1+1+1', gc='no_covering_barrier', blobs=1,
                      logical_bytes=4096, observed_replica_bytes=4096, missing_metadata_blobs=0,
                      phantom_like=False, inv1_candidate=True, inv2_candidate=True)
        report = dict(version=1, group_id=self.gid, completed_vdisks=list(range(6)),
                      buckets=[bucket], totals=write2x2_report.totals([bucket]))
        self.master.rpc('/result', dict(worker='worker-1', group=self.gid, token=job['token'], result=dict(
            candidates={'inv1_candidate': ['[42:7:258:3:5:4096:0]'],
                        'inv2_candidate': ['[42:7:258:3:5:4096:0]']},
            candidates_complete=True, completed_vdisks=list(range(6)), missing_vdisks=[6, 7, 8],
            scan_report=report, started_at=now - 1, finished_at=now)))
        app = self.app()
        async with app.run_test(size=(150, 40)) as pilot:
            await self.settle(app, pilot)
            await self.tab(app, pilot, 'candidates')
            table = app.query_one('#candidate-table', DataTable)
            self.assertEqual(table.row_count, 2)
            self.assertEqual({str(table.get_row_at(i)[1]) for i in range(table.row_count)},
                             {'inv1_candidate', 'inv2_candidate'})
            app.query_one('#candidate-category', Select).value = 'inv1_candidate'
            await self.settle(app, pilot)
            self.assertEqual(table.row_count, 1)
            self.assertEqual(str(table.get_row_at(0)[2]), '[42:7:258:3:5:4096:0]')
            table.focus()
            await pilot.press('enter')
            await self.settle(app, pilot)
            self.assertEqual(app.detail_target, ('group', str(self.gid)))
            await self.tab(app, pilot, 'candidates')
            self.assertIn('candidates inv1_candidate %d 0 1000' % self.gid, self.connection.commands)
            await pilot.click('#candidate-selected-group')
            await self.settle(app, pilot)
            self.assertEqual(self.connection.commands[-1], 'candidates inv1_candidate all 0 1000')

            original_request = self.connection.request

            async def request(command):
                if not command.startswith('candidates '):
                    return await original_request(command)
                self.connection.commands.append(command)
                _, category, group, offset, limit = command.split()
                offset, limit = int(offset), int(limit)
                rows = [dict(gid=self.gid, category=category, blob=f'[42:7:{i}:3:5:4096:0]',
                             first_seen=time.time(), last_seen=time.time(), coverage=list(range(6)))
                        for i in range(offset, min(offset + limit, 1001))]
                return json.dumps(dict(category=category, group=None, offset=offset, limit=limit,
                                       total=1001, rows=rows))

            with patch.object(self.connection, 'request', side_effect=request):
                app.action_refresh()
                await self.settle(app, pilot)
                self.assertEqual(table.row_count, 1000)
                self.assertTrue(app.query_one('#candidate-previous', Button).disabled)
                self.assertFalse(app.query_one('#candidate-next', Button).disabled)
                await pilot.click('#candidate-next')
                await self.settle(app, pilot)
                self.assertEqual(table.row_count, 1)
                self.assertIn('1,001–1,001 of 1,001', str(app.query_one('#candidate-summary', Static).content))
                self.assertFalse(app.query_one('#candidate-previous', Button).disabled)
                self.assertTrue(app.query_one('#candidate-next', Button).disabled)
                await pilot.click('#candidate-previous')
                await self.settle(app, pilot)
                self.assertEqual(table.row_count, 1000)
                app.query_one('#candidate-category', Select).value = 'inv2_candidate'
                await self.settle(app, pilot)
                self.assertEqual(app._candidate_offset, 0)
                self.assertEqual(self.connection.commands[-1], 'candidates inv2_candidate all 0 1000')

    async def test_invariant_filters_are_independent_and_overlap(self):
        def bucket(inv1_candidate, blobs):
            return dict(local='0+1+?' if inv1_candidate else '1+1+?', ingress='1+1+1',
                        gc='no_covering_barrier', blobs=blobs, logical_bytes=1024 * blobs,
                        observed_replica_bytes=(1 if inv1_candidate else 2) * 1024 * blobs,
                        missing_metadata_blobs=0, phantom_like=False, inv1_candidate=inv1_candidate, inv2_candidate=True)

        self.replace_report([bucket(True, 2), bucket(False, 3)])
        app = self.app()
        async with app.run_test(size=(150, 40)) as pilot:
            await self.open_pool(app, pilot)
            table = app.query_one('#layout-table', DataTable)
            self.assertEqual(sum(int(str(table.get_row_at(i)[1])) for i in range(table.row_count)), 5)
            await pilot.click('#inv2-candidate-only')
            await pilot.pause()
            self.assertEqual(sum(int(str(table.get_row_at(i)[1])) for i in range(table.row_count)), 5)
            await pilot.click('#inv1-candidate-only')
            await pilot.pause()
            self.assertEqual(str(table.get_row_at(0)[1]), '2')
            await pilot.click('#inv2-candidate-only')
            await pilot.pause()
            self.assertEqual(str(table.get_row_at(0)[1]), '2')

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
    def test_sizes_choose_units(self):
        self.assertEqual([size(n) for n in (0, 100, 1024, 1024**2, 1024**3)],
                         ['0 B', '100 B', '1.0 KiB', '1.0 MiB', '1.0 GiB'])

    def test_dispatch_needs_no_service_database_or_cluster_connection(self):
        args = SimpleNamespace(blobsan_service_action='client', console_sock='/tmp/test.sock', refresh=3,
                               request_timeout=7, dry_run=False)
        with patch('ydb.apps.dstool.tui.client.ConsoleApp') as app:
            command.do(args)
        app.assert_called_once_with('/tmp/test.sock', refresh=3, timeout=7)
        app.return_value.run.assert_called_once_with()

    def test_dry_run_rejected_before_starting_client(self):
        with patch('ydb.apps.dstool.tui.client.ConsoleApp') as app, \
                self.assertRaises(common.InvalidParameterError):
            command.do(SimpleNamespace(blobsan_service_action='client', dry_run=True))
        app.assert_not_called()

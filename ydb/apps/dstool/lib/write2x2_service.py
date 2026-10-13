"""Coordinator and worker runtime for cluster write2x2service (standard library)."""

import argparse
import collections
import concurrent.futures
import hashlib
import hmac
from http import server as http_server
import json
import logging
import os
from pathlib import Path

import re
import shlex
import shutil
import socket
import socketserver
import sqlite3
import ssl
import struct
import subprocess
import threading
import time
import urllib.parse
import urllib.request
import uuid

from ydb.apps.dstool.lib import common


LOG = logging.getLogger('w2x2')
STOP = threading.Event()
MAX_BODY = 128 * 1024 * 1024
CANDIDATE = re.compile(r'GroupId#\s+(\d+)\s+BlobId#\s+(\[(?:\d+:){6}\d+\])')


def encode(value):
    return json.dumps(value, separators=(',', ':')).encode()


def atomic_json(path, value):
    tmp = path.with_suffix('.tmp')
    with tmp.open('wb') as out:
        out.write(encode(value))
        out.flush()
        os.fsync(out.fileno())
    tmp.replace(path)
    fd = os.open(str(path.parent), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def secret(path):
    return Path(path).expanduser().read_text().strip() if path else ''


def token_headers():
    params = common.connection_params
    if not params.token:
        return {}
    value = (params.token_type + ' ' if params.token_type else '') + params.token
    return {'Authorization': value}


def http(url, headers=None, data=None, timeout=30, raw=False, context=None):
    req = urllib.request.Request(url, data=None if data is None else encode(data), headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout, context=context) as response:
        body = response.read(MAX_BODY + 1)
    if len(body) > MAX_BODY:
        raise ValueError('HTTP response exceeds 128 MiB')
    return body if raw else json.loads(body)


def viewer_url(args):
    if args.viewer:
        return args.viewer.rstrip('/')
    params = common.connection_params
    endpoint = params.make_endpoint_info(args.endpoint[0])
    protocol = endpoint.protocol if endpoint.protocol in ('http', 'https') else params.mon_protocol
    return protocol + '://' + endpoint.host_with_mon_port


def view(args, path, **params):
    return ydb_http(args, viewer_url(args) + '/' + path + '?' + urllib.parse.urlencode(params))


def ydb_http(args, url, raw=False):
    params = common.connection_params
    context = ssl.create_default_context(cafile=params.cafile)
    if params.insecure:
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    return http(url, token_headers(), timeout=args.request_timeout, raw=raw, context=context)


def topology(args):
    """BSC inventory for dynamic groups, whiteboard for static groups."""
    data = common.fetch_base_config_and_storage_pools(cache=False)
    base = data['BaseConfig']
    pools = {(p.BoxId, p.StoragePoolId): p.Name for p in data['StoragePools']}
    slots = {(s.VSlotId.NodeId, s.VSlotId.PDiskId, s.VSlotId.VSlotId): s for s in base.VSlot}
    groups = {}
    for row in base.Group:
        if row.ErasureSpecies != 9 or row.HasField('VirtualGroupInfo'):
            continue
        group = dict(id=row.GroupId, generation=row.GroupGeneration,
                     pool=pools.get((row.BoxId, row.StoragePoolId)), slots=[])
        for id_ in row.VSlotId:
            slot = slots[id_.NodeId, id_.PDiskId, id_.VSlotId]
            if slot.GroupId != row.GroupId or slot.GroupGeneration != row.GroupGeneration:
                raise ValueError('Inconsistent BSC layout')
            if slot.VDiskIdx != 0 or slot.FailRealmIdx not in range(3) or slot.FailDomainIdx not in range(3):
                raise ValueError('Only 3x3 mirror-3-dc geometry is supported')
            group['slots'].append(dict(index=slot.FailRealmIdx * 3 + slot.FailDomainIdx,
                                       node=id_.NodeId, pdisk=id_.PDiskId, slot=id_.VSlotId))
        groups[row.GroupId] = group
    infos = view(args, 'viewer/json/bsgroupinfo').get('BSGroupStateInfo', [])
    static = [g for g in infos if int(g['GroupID']) < 0x80000000 and g.get('ErasureSpecies') == 'mirror-3-dc']
    static.sort(key=lambda g: int(g['GroupGeneration']), reverse=True)
    if static:
        rows = view(args, 'viewer/json/vdiskinfo').get('VDiskStateInfo', [])
        for info in static:
            gid, generation = int(info['GroupID']), int(info['GroupGeneration'])
            if gid in groups:
                continue
            group = dict(id=gid, generation=generation, pool='static', slots=[])
            for row in rows:
                v = row.get('VDiskId', {})
                if int(v.get('GroupID', -1)) != gid or int(v.get('GroupGeneration', -1)) != generation or row.get('DonorMode'):
                    continue
                if int(v.get('VDisk', 0)) != 0:
                    raise ValueError('Only 3x3 geometry is supported')
                group['slots'].append(dict(index=int(v.get('Ring', 0)) * 3 + int(v.get('Domain', 0)),
                                           node=int(row['NodeId']), pdisk=int(row['PDiskId']),
                                           slot=int(row['VDiskSlotId'])))
            groups[gid] = group
    for group in groups.values():
        group['slots'].sort(key=lambda s: s['index'])
        indexes = [s['index'] for s in group['slots']]
        minimum = 6 if group['pool'] == 'static' else 9
        if len(indexes) < minimum or len(set(indexes)) != len(indexes) or any(i not in range(9) for i in indexes):
            raise ValueError('Incomplete/ambiguous layout for group %s' % group['id'])
        group['fingerprint'] = hashlib.sha256(encode(group)).hexdigest()
    if not groups:
        raise ValueError('No mirror-3-dc groups found')
    return list(groups.values())


class Master:
    def __init__(self, args, directory):
        self.args = args
        self.lock = threading.RLock()
        self.refresh = threading.Event()
        self.db = sqlite3.connect(str(directory / 'state.sqlite3'), check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.executescript('''
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT);
            CREATE TABLE IF NOT EXISTS groups(
                id INTEGER PRIMARY KEY, layout TEXT, seen REAL, due REAL DEFAULT 0,
                token TEXT, owner TEXT, expires REAL DEFAULT 0, last_token TEXT,
                last_scan REAL, last_count INTEGER, last_result TEXT, error TEXT,
                scans INTEGER DEFAULT 0, new_total INTEGER DEFAULT 0);
            CREATE TABLE IF NOT EXISTS workers(id TEXT PRIMARY KEY, seen REAL, info TEXT);
            CREATE TABLE IF NOT EXISTS findings(
                gid INTEGER, blob TEXT, first_seen REAL, last_seen REAL, active INTEGER,
                status TEXT, PRIMARY KEY(gid,blob));
        ''')
        for key, value in [('paused', 0), ('interval', args.interval), ('retry', args.retry)]:
            self.db.execute('INSERT OR IGNORE INTO settings VALUES(?,?)', (key, str(value)))
        self.db.commit()

    def setting(self, key):
        return float(self.db.execute('SELECT value FROM settings WHERE key=?', (key,)).fetchone()[0])

    def discover(self):
        while not STOP.is_set():
            self.refresh.clear()
            try:
                layouts = topology(self.args)
                now = time.time()
                with self.lock, self.db:
                    for layout in layouts:
                        gid = layout['id']
                        old = self.db.execute('SELECT layout FROM groups WHERE id=?', (gid,)).fetchone()
                        if old and json.loads(old[0])['fingerprint'] != layout['fingerprint']:
                            self.db.execute('UPDATE groups SET token=NULL,expires=0,due=0 WHERE id=?', (gid,))
                        self.db.execute('''INSERT INTO groups(id,layout,seen) VALUES(?,?,?)
                            ON CONFLICT(id) DO UPDATE SET layout=excluded.layout,seen=excluded.seen''',
                                        (gid, json.dumps(layout), now))
                    self.db.execute('INSERT OR REPLACE INTO settings VALUES(?,?)', ('discovery_error', ''))
                    self.db.execute('INSERT OR REPLACE INTO settings VALUES(?,?)', ('discovered_at', str(now)))
                LOG.info('Discovered %d groups', len(layouts))
            except Exception as exc:
                LOG.exception('Discovery failed')
                with self.lock, self.db:
                    self.db.execute('INSERT OR REPLACE INTO settings VALUES(?,?)', ('discovery_error', str(exc)))
            self.refresh.wait(self.args.refresh)

    def rpc(self, action, body):
        now = time.time()
        worker = str(body['worker'])
        if len(worker) > 200:
            raise ValueError('Invalid worker ID')
        with self.lock, self.db:
            if action not in ('/claim', '/heartbeat', '/result'):
                raise ValueError('Unknown operation')
            self.db.execute('INSERT OR REPLACE INTO workers VALUES(?,?,?)', (worker, now, str(body.get('info', ''))[:500]))
            if action == '/claim':
                # A lost claim response must not allocate a second group to the worker.
                owned = self.db.execute('SELECT * FROM groups WHERE owner=? AND token IS NOT NULL AND expires>?',
                                        (worker, now)).fetchone()
                if owned:
                    return {'job': dict(layout=json.loads(owned['layout']), token=owned['token'], lease_seconds=owned['expires'] - now)}
                if self.setting('paused'):
                    return {'job': None}
                busy = set()
                for row in self.db.execute('SELECT layout FROM groups WHERE token IS NOT NULL AND expires>?', (now,)):
                    busy.update(s['node'] for s in json.loads(row[0])['slots'])
                rows = self.db.execute('''SELECT * FROM groups WHERE due<=? AND
                    (token IS NULL OR expires<=?) AND seen>? ORDER BY due,id''',
                                       (now, now, now - self.args.refresh * 2)).fetchall()
                for row in rows:
                    layout = json.loads(row['layout'])
                    if busy.intersection(s['node'] for s in layout['slots']):
                        continue
                    lease = uuid.uuid4().hex
                    self.db.execute('UPDATE groups SET token=?,owner=?,expires=? WHERE id=?',
                                    (lease, worker, now + self.args.lease, row['id']))
                    return {'job': dict(layout=layout, token=lease, lease_seconds=self.args.lease)}
                return {'job': None}
            gid, lease = int(body['group']), body['token']
            row = self.db.execute('SELECT * FROM groups WHERE id=?', (gid,)).fetchone()
            if action == '/result' and row and row['last_token'] == lease:
                return {'accepted': True}
            if not row or row['token'] != lease or row['owner'] != worker or row['expires'] <= now:
                return {'accepted': False}
            if action == '/heartbeat':
                self.db.execute('UPDATE groups SET expires=? WHERE id=?', (now + self.args.lease, gid))
                return {'accepted': True}
            if action != '/result':
                raise ValueError('Unknown operation')
            result = body['result']
            error = result.get('error')
            if not error:
                findings = result['findings']
                if not isinstance(findings, dict) or any(not re.fullmatch(r'\[(?:\d+:){6}0\]', b)
                                                        or not isinstance(s, str) for b, s in findings.items()):
                    raise ValueError('Invalid findings')
                completed = result['completed_vdisks']
                if not isinstance(completed, list) or any(i not in range(9) for i in completed) or len(set(completed)) != len(completed):
                    raise ValueError('Invalid coverage')
                if sum(all(r * 3 + d in completed for d in range(3)) for r in range(3)) < 2:
                    raise ValueError('Incomplete scan')
                previous = {r['blob'] for r in self.db.execute('SELECT blob FROM findings WHERE gid=? AND active=1', (gid,))}
                prior = json.loads(row['last_result'] or '{}').get('completed_vdisks', [])
                comparable = len(completed) == 9 or set(completed) == set(prior)
                if comparable:
                    self.db.execute('UPDATE findings SET active=0 WHERE gid=?', (gid,))
                new = 0
                for blob, status in findings.items():
                    new += blob not in previous
                    self.db.execute('''INSERT INTO findings VALUES(?,?,?,?,1,?)
                        ON CONFLICT(gid,blob) DO UPDATE SET last_seen=excluded.last_seen,active=1,status=excluded.status''',
                                    (gid, blob, now, now, status))
                result = {k: v for k, v in result.items() if k != 'findings'}
                result.update(candidates=len(findings), new=new,
                              disappeared=len(previous - findings.keys()) if comparable else 0)
                self.db.execute('''UPDATE groups SET last_scan=?,last_count=?,scans=scans+1,
                    new_total=new_total+? WHERE id=?''', (now, len(findings), new, gid))
                delay = self.setting('retry' if findings else 'interval')
                if new:
                    LOG.warning('Group %s: %d new/reappeared candidates, %d total', gid, new, len(findings))
            else:
                delay = self.setting('retry')
                LOG.warning('Group %s failed: %s', gid, error)
            self.db.execute('''UPDATE groups SET token=NULL,expires=0,last_token=?,due=?,
                last_result=?,error=? WHERE id=?''', (lease, now + delay, json.dumps(result), error, gid))
            return {'accepted': True}

    def console(self, command):
        words = shlex.split(command)
        if not words:
            words = ['stats']
        op = words[0]
        with self.lock, self.db:
            if op in ('pause', 'resume'):
                self.db.execute('UPDATE settings SET value=? WHERE key="paused"', (str(int(op == 'pause')),))
            elif op in ('interval', 'retry'):
                value = positive(words[1])
                self.db.execute('UPDATE settings SET value=? WHERE key=?', (str(value), op))
            elif op == 'scan':
                if len(words) == 1 or words[1] == 'all':
                    self.db.execute('UPDATE groups SET due=0')
                else:
                    self.db.execute('UPDATE groups SET due=0 WHERE id=?', (int(words[1]),))
            elif op == 'refresh':
                self.refresh.set()
            elif op == 'stats':
                now = time.time()
                stats = dict(self.db.execute('SELECT key,value FROM settings'))
                stats.update(dict(self.db.execute('''SELECT count(*) groups,
                    sum(token IS NOT NULL AND expires>?) busy, sum(last_scan IS NULL) never_scanned,
                    sum(error IS NOT NULL) failed, sum(last_count) candidates_at_last_scans,
                    min(last_scan) oldest_scan, sum(scans) scans, sum(new_total) new_total
                    FROM groups''', (now,)).fetchone()))
                stats['workers_alive'] = self.db.execute('SELECT count(*) FROM workers WHERE seen>?',
                                                        (now - self.args.lease,)).fetchone()[0]
                stats['stale_inventory_groups'] = self.db.execute('SELECT count(*) FROM groups WHERE seen<?',
                                                                (now - self.args.refresh * 2,)).fetchone()[0]
                stats['unresolved_findings'] = self.db.execute('SELECT count(*) FROM findings WHERE active=1').fetchone()[0]
                stats['scans_with_missing_vdisks'] = sum(bool(json.loads(r[0]).get('missing_vdisks'))
                                                        for r in self.db.execute('SELECT last_result FROM groups WHERE last_result IS NOT NULL'))
                if stats['oldest_scan'] is not None:
                    stats['oldest_scan_age_seconds'] = round(now - stats.pop('oldest_scan'))
                return json.dumps(stats, indent=2)
            elif op in ('groups', 'workers', 'findings'):
                if op == 'groups':
                    query, values = ('SELECT id,owner,expires,due,last_scan,last_count,error,last_result FROM groups ORDER BY id', ())
                elif op == 'workers':
                    query, values = ('SELECT * FROM workers ORDER BY seen DESC', ())
                elif len(words) > 1:
                    query, values = ('SELECT * FROM findings WHERE active=1 AND gid=? LIMIT 1000', (int(words[1]),))
                else:
                    query, values = ('SELECT * FROM findings WHERE active=1 LIMIT 1000', ())
                return '\n'.join(json.dumps(dict(r)) for r in self.db.execute(query, values)) or '(empty)'
            elif op == 'help':
                return 'stats | groups | workers | findings [GROUP] (max 1000) | pause | resume | scan [GROUP|all] | interval SEC | retry SEC | refresh | quit'
            else:
                raise ValueError('Unknown command; use help')
        return 'OK'


class API(http_server.BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            self.connection.settimeout(60)
            expected = self.server.api_secret
            if expected and not hmac.compare_digest(self.headers.get('Authorization', ''), 'Bearer ' + expected):
                self.send_error(403)
                return
            size = int(self.headers.get('Content-Length', '0'))
            if not 0 < size <= MAX_BODY:
                self.send_error(413)
                return
            result = encode(self.server.master.rpc(self.path, json.loads(self.rfile.read(size))))
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(result)))
            self.end_headers()
            self.wfile.write(result)
        except Exception as exc:
            LOG.exception('API request failed')
            self.send_error(400, str(exc)[:200])

    def log_message(self, *_):
        pass


class Console(socketserver.StreamRequestHandler):
    def handle(self):
        def send(text):
            self.wfile.write((text.replace('\n', '\r\n') + '\r\n> ').encode())
        try:
            send('w2x2 console. help for commands.\n' + self.server.master.console('stats'))
            while not STOP.is_set():
                line = self.rfile.readline(4096)
                if not line or line.strip() in (b'quit', b'exit'):
                    return
                # Ignore initial telnet option negotiation (IAC verb option).
                line = re.sub(b'\xff[\xfb-\xfe].', b'', line)
                try:
                    send(self.server.master.console(line.decode().strip()))
                except Exception as exc:
                    send('ERROR: ' + str(exc))
        except (OSError, UnicodeError):
            pass


class UnixServer(socketserver.ThreadingUnixStreamServer):
    daemon_threads = True


def serve(args, directory):
    master = Master(args, directory)
    server = http_server.ThreadingHTTPServer((args.bind, args.listen), API)
    server.master, server.api_secret = master, secret(args.secret_file)
    path = Path(args.console_sock).expanduser()
    if path.exists():
        # Never unlink another live console, or an unrelated file.
        if not path.is_socket():
            raise ValueError('Console path exists and is not a socket')
        with socket.socket(socket.AF_UNIX) as probe:
            try:
                probe.connect(str(path))
            except ConnectionRefusedError:
                path.unlink()
            else:
                raise ValueError('Console socket is already in use')
    console = UnixServer(str(path), Console)
    console.master = master
    path.chmod(0o777)
    for target in (server.serve_forever, console.serve_forever, master.discover):
        threading.Thread(target=target, daemon=True).start()
    LOG.info('Master listening on %s:%d; console %s', args.bind, args.listen, path)
    try:
        STOP.wait()
    finally:
        master.refresh.set()
        server.shutdown()
        console.shutdown()
        server.server_close()
        console.server_close()
        path.unlink(missing_ok=True)


def check_running(cancel):
    if STOP.is_set() or cancel.is_set() or time.monotonic() >= getattr(cancel, 'deadline', float('inf')):
        raise RuntimeError('Stopped or lost master lease')


def scan(args, job, directory, cancel):
    layout, base = job['layout'], viewer_url(args)
    deadline = time.monotonic() + args.scan_timeout

    def fetch(slot):
        index = slot['index']
        params = dict(pdiskId=slot['pdisk'], vdiskSlotId=slot['slot'], sessionId=uuid.uuid4().hex)
        url = base + '/node/%d/vdisk_stream?' % slot['node'] + urllib.parse.urlencode(params)
        with (directory / ('slot-%d' % index)).open('wb') as out:
            while True:
                check_running(cancel)
                if time.monotonic() >= deadline:
                    raise TimeoutError('snapshot timeout')
                # No retries: a lost response has already advanced the server stream.
                data = ydb_http(args, url, raw=True)
                if not data:
                    return index
                if data == b'ERROR':
                    raise ValueError('VDisk stream returned ERROR')
                out.write(struct.pack('<III', layout['id'], index, len(data)))
                out.write(data)

    good, errors = [], {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=9) as pool:
        futures = {pool.submit(fetch, slot): slot['index'] for slot in layout['slots']}
        for future in concurrent.futures.as_completed(futures):
            try:
                good.append(future.result())
            except Exception as exc:
                errors[futures[future]] = str(exc)
    check_running(cancel)
    realms = [r for r in range(3) if all(r * 3 + d in good for d in range(3))]
    if len(realms) < 2:
        raise RuntimeError('Need two complete realms; completed=%s errors=%s' % (sorted(good), errors))
    # Reproduce dstool take-snapshot framing without interpreting VDisk metadata.
    snapshot = directory / 'snapshot'
    with snapshot.open('wb') as out:
        for index in sorted(good):
            check_running(cancel)
            with (directory / ('slot-%d' % index)).open('rb') as inp:
                shutil.copyfileobj(inp, out)
    candidates = run_blobsan(args, layout['id'], directory, cancel)
    return candidates, dict(completed_vdisks=sorted(good), missing_vdisks=sorted(set(range(9)) - set(good)), stream_errors=errors)


def run_blobsan(args, gid, directory, cancel):
    """The supplied executable alone decides which blobs need repair."""
    check_running(cancel)
    report = directory / 'blobsan.err'
    with (directory / 'snapshot').open('rb') as inp, report.open('wb') as err:
        process = subprocess.Popen([str(Path(args.blobsan).expanduser()), 'mirror-3-dc'],
                                   stdin=inp, stdout=subprocess.DEVNULL, stderr=err)
        try:
            deadline = time.monotonic() + args.scan_timeout
            while process.poll() is None:
                check_running(cancel)
                if time.monotonic() >= deadline:
                    raise TimeoutError('blobsan timeout')
                cancel.wait(0.2)
            if process.returncode:
                raise RuntimeError('blobsan exited %d; see %s' % (process.returncode, report))
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
    check_running(cancel)
    candidates = set()
    with report.open(errors='replace') as inp:
        for line in inp:
            if 'GroupId#' not in line:
                continue
            match = CANDIDATE.search(line)
            if not match or int(match[1]) != gid or match[2].split(':')[-1] != '0]':
                raise ValueError('Unrecognized blobsan candidate: ' + line[:200])
            candidates.add(match[2])
    return candidates


def validate_layout(args, job):
    layout, base = job['layout'], viewer_url(args)
    groups = ydb_http(args, base + '/viewer/json/bsgroupinfo').get('BSGroupStateInfo', [])
    groups = [g for g in groups if int(g['GroupID']) == layout['id']]
    group = max(groups, key=lambda g: int(g['GroupGeneration'])) if groups else None
    if group is None or int(group['GroupGeneration']) != layout['generation']:
        raise ValueError('Group generation changed or cannot be verified')
    if group.get('ErasureSpecies') != 'mirror-3-dc':
        raise ValueError('Group erasure changed')


def repair(args, job, candidates, cancel):
    layout, base = job['layout'], viewer_url(args)
    if not candidates:
        return {}
    tenants = ydb_http(args, base + '/viewer/json/tenantinfo').get('TenantInfo', [])
    systems = ydb_http(args, base + '/viewer/json/sysinfo').get('SystemStateInfo', [])
    pool = layout.get('pool') or 'static'
    tenant = pool.rpartition(':')[0] if ':' in pool else None
    nodes = {int(n) for t in tenants if t['Name'] == tenant for n in t.get('NodeIds', [])}
    nodes.update(int(s['NodeId']) for s in systems if tenant and tenant in s.get('Tenants', []))
    if not nodes:
        nodes = {int(s['NodeId']) for s in systems if not s.get('Tenants')}
    if not nodes:
        raise ValueError('No proxy nodes available for group')
    nodes = sorted(nodes)
    results = {}
    for i, blob in enumerate(sorted(candidates)):
        check_running(cancel)
        params = dict(groupId=layout['id'], tabletId=blob[1:].split(':')[0],
                      mustRestoreFirst=1, json=1, **{'from': blob, 'to': blob})
        try:
            response = ydb_http(args, base + '/node/%d/blob_range?' % nodes[i % len(nodes)] + urllib.parse.urlencode(params))
            if response.get('Status') != 'OK' or response.get('Blobs') not in ([], [blob]):
                raise ValueError('Unexpected response: ' + str(response)[:300])
            results[blob] = 'request_ok' if response['Blobs'] else 'absent'
        except Exception as exc:
            results[blob] = 'failed: ' + str(exc)[:300]
        if args.repair_delay:
            cancel.wait(args.repair_delay)
    return results


def work(args, directory):
    state = directory / 'worker.json'
    if not state.exists():
        atomic_json(state, {'id': socket.gethostname() + '-' + uuid.uuid4().hex[:12]})
    worker = json.loads(state.read_text())['id']
    pending = directory / 'pending.json'
    base = args.master if '://' in args.master else 'http://' + args.master
    auth = secret(args.secret_file)

    def rpc(action, **body):
        return http(base.rstrip('/') + '/' + action, {'Authorization': 'Bearer ' + auth},
                    dict(worker=worker, info=socket.gethostname(), **body), timeout=args.request_timeout)

    while not STOP.is_set():
        try:
            if pending.exists():
                body = json.loads(pending.read_text())
                response = rpc('result', **body)
                LOG.info('Pending result group %s: %s', body['group'], response)
                pending.unlink()
                continue
            claim_started = time.monotonic()
            job = rpc('claim')['job']
            if not job:
                STOP.wait(args.poll)
                continue
            gid, lease = job['layout']['id'], job['token']
            cancel, finished = threading.Event(), threading.Event()
            lease_seconds = job['lease_seconds']
            cancel.deadline = claim_started + lease_seconds

            def heartbeat():
                while not finished.wait(min(args.heartbeat, lease_seconds / 3)):
                    try:
                        start = time.monotonic()
                        if not rpc('heartbeat', group=gid, token=lease)['accepted']:
                            cancel.set()
                            return
                        cancel.deadline = start + lease_seconds
                    except Exception:
                        # No new writes while ownership cannot be confirmed.
                        cancel.set()
                        return

            thread = threading.Thread(target=heartbeat, daemon=True)
            thread.start()
            started = time.monotonic()
            scratch = directory / 'scan'
            LOG.info('Scanning group %s', gid)
            try:
                shutil.rmtree(scratch, ignore_errors=True)
                scratch.mkdir()
                check_running(cancel)
                validate_layout(args, job)
                candidates, coverage = scan(args, job, scratch, cancel)
                validate_layout(args, job)
                try:
                    findings = repair(args, job, candidates, cancel)
                except Exception as exc:
                    check_running(cancel)
                    findings = {blob: 'failed: ' + str(exc)[:300] for blob in candidates}
                check_running(cancel)
                validate_layout(args, job)
                result = dict(findings=findings, **coverage)
                result['repairs'] = dict(collections.Counter(s.split(':')[0] for s in findings.values()))
                LOG.info('Group %s: %d candidates, repairs %s', gid, len(candidates), result['repairs'])
            except Exception as exc:
                LOG.exception('Group %s failed', gid)
                result = {'error': str(exc)}
            finally:
                finished.set()
                thread.join()
            result['seconds'] = round(time.monotonic() - started, 2)
            atomic_json(pending, dict(group=gid, token=lease, result=result))
        except Exception:
            LOG.exception('Worker operation failed; retrying')
            STOP.wait(args.poll)


def positive(value):
    value = int(value)
    if value <= 0:
        raise argparse.ArgumentTypeError('Must be positive')
    return value


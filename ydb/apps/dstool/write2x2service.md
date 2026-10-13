# Continuous single-DC blob repair

`dstool cluster write2x2service` runs outside `ydbd`. It discovers conventional
`mirror-3-dc` groups, scans VDisk metadata, and issues `blob_range` requests with
`mustRestoreFirst=1` for blobs whose local parts are visible in only one realm.
It repeats the scan after repair and continues monitoring for new candidates.
The service lives in the built `dstool` binary. Workers also need the patched
`blobsan` executable from `blobstorage-scripts/write2x2/`. It is invoked as a
black box; the service does not reproduce its analysis logic. No Python
installation, shared database, or new server code is required. The service uses
Python's standard library and dstool's existing connection/configuration code.
Master, worker, and the console require POSIX (Linux is the deployment target).

## Start

```bash
dstool --token-file ~/.token cluster write2x2service master \
    -l 22570 --db ~/w2x2_master_db/ --console-sock /tmp/w2x2.sock

# On each selected storage host:
dstool --token-file ~/.token cluster write2x2service worker \
    --master master-host:22570 --db ~/w2x2_worker_db/ --blobsan ~/blobsan
```

For this command only, an omitted `-e` defaults to `localhost`. Each process
uses its own endpoint and credentials. All workers must point to the **same
cluster** as the master. Global `-e`, `--mon-port`, TLS and token options work
as usual. `--viewer URL` overrides the HTTP Viewer URL; the master still uses
its global endpoint for BSC discovery. Global token defaults/environment
variables follow dstool conventions; `~/.token` is used only when supplied
explicitly. Tokens are read at startup; restart the process to rotate them.
`worker --blobsan PATH` defaults to `~/blobsan`. The master does not need it.

Choose separate persistent directories for master and each worker. A file lock
prevents two processes using the same directory. Run under your normal process
supervisor for restart after host/process failure. Signals SIGINT/SIGTERM stop
the service; an in-flight HTTP operation can run until its timeout.

## Deploy

From the `ydb` directory, after building:

```bash
for host in host1 host2; do scp ./apps/dstool/ydb-dstool "$host:dstool.new" && ssh "$host" 'chmod +x ~/dstool.new && mv -f ~/dstool.new ~/dstool'; done
```

The temporary file allows replacing a running executable. Restart services
separately to use the new binary. Credentials are not copied.

To copy both executables to worker hosts:

```bash
for host in host1 host2; do scp ./apps/dstool/ydb-dstool "$host:dstool.new" && scp ./blobstorage-scripts/write2x2/blobsan "$host:blobsan.new" && ssh "$host" 'chmod +x ~/dstool.new ~/blobsan.new && mv -f ~/dstool.new ~/dstool && mv -f ~/blobsan.new ~/blobsan'; done
```

## Console

The Unix socket is created with mode **0777**, as requested. Anyone who can
access it can run console commands. No shell commands are executed by the
console. Use `telnet -U /tmp/w2x2.sock` if your telnet supports Unix sockets,
or `socat - UNIX-CONNECT:/tmp/w2x2.sock`. Stats are printed on connection.

| Command | Effect |
|---|---|
| `stats` | Discovery status, scans, candidate totals, unresolved findings, live workers, stale inventory and missing VDisks |
| `groups` | Per-group scan time, candidate count, coverage, repair response counts, owner, scheduling and errors |
| `workers` | Known workers and last contact timestamps |
| `findings [GROUP]` | Up to 1000 unresolved candidates with last repair response |
| `pause` / `resume` | Stop/resume assigning new jobs; running jobs finish |
| `scan [GROUP\|all]` | Make a group/all groups eligible for scanning now |
| `interval SECONDS` | Change the clean-group rescan interval |
| `retry SECONDS` | Change the delay after candidates or scan errors |
| `refresh` | Refresh group topology now |
| `help` / `quit` | Show commands / disconnect |

Intervals and pause state persist in SQLite. `--interval` (default 3600 seconds)
and `--retry` (60) initialize a new database; subsequently use the console.
Changed intervals apply when scheduling the next completed job; `scan all`
also resets previously scheduled due times. The console is request/response,
not a continuous terminal dashboard. Times in detailed rows are Unix timestamps.

## Scheduling and persistence

One worker processes one group at a time. The master avoids concurrent jobs
sharing storage nodes. This is intentionally conservative; tune concurrency
after measuring real groups. All VDisks of one group can stream concurrently.
Repair requests are sequential; `worker --repair-delay SECONDS` adds spacing.

Master SQLite stores topology, leases, schedules, findings and the latest
summary per group. Workers persist their identity and unacknowledged report in
their local directory. Compressed snapshot files for the last job remain in
`DB/scan` and are replaced by the next job. The assembled input is `snapshot`;
the analyzer's full stderr is `blobsan.err`. Resource usage depends on group
size, the external analyzer and retained findings.

Jobs have renewable leases (`master --lease`, default 120 seconds). Failed
workers' jobs become available after lease expiry. Claim retries return the
same assignment; duplicate reports are acknowledged without applying twice;
expired/reassigned reports are rejected. Worker heartbeat interval defaults to
20 seconds and is capped at one third of the granted lease. Loss of contact
stops new repair requests; an already issued request cannot be revoked in
`ydbd`. The master itself is a single coordinator, without automatic failover.

`master --refresh` controls topology discovery (300 seconds). Stale inventory
older than twice this interval is not assigned. Generation changes invalidate
old jobs. Workers verify group generation before scanning, before repairs and
after repairs. The configured geometry must be 3 realms x 3 domains x 1 VDisk.
Static groups are discovered through Viewer; an unavailable static VDisk may
be absent from that inventory. Dynamic virtual/blob-depot groups are excluded.

The API listens on all interfaces by default (`--bind` changes this). Use it on
a trusted network. Optional `--secret-file` on master and workers enables
shared-secret authentication. The built-in master API is HTTP, not TLS; it
should not be exposed to an untrusted network. The cluster token is never sent
to the master by a worker.

## What a successful pass means

The worker downloads `vdisk_stream` responses until the endpoint returns an
empty response. HTTP failures and explicit `ERROR` responses discard that
VDisk's stream; the next attempt creates a new session. A session request is
never retried because the server may already have advanced its cursor. The
worker adds the same group/index/length framing as `dstool group take-snapshot`,
without decoding the metadata payload.

A pass requires three complete VDisk streams in each of at least two realms.
The missing indexes and errors are recorded, even for an accepted pass. A
six-VDisk scan is not a health certificate for all nine VDisks. Snapshots across
VDisk are not atomic. Empty initial streams are valid empty indexes.

The assembled snapshot is passed as stdin to `blobsan mirror-3-dc`. Stdout goes
to `/dev/null`, as in the original scripts (this also disables verbose output
in blobsan). Stderr is saved and only `GroupId# ... BlobId# ...` lines are parsed.
The supplied executable is the sole authority on candidate selection and
snapshot payload validation. A nonzero exit, timeout, or malformed candidate
line fails the pass. The process is terminated on cancellation or lease loss.
The service itself does not check ingress, GC barriers, or replication layout
inside the snapshot. Transport completion plus analyzer success is not an
independent proof of complete or consistent metadata. Use the intended patched
analyzer; a different executable can produce different candidates.

`request_ok` means `blob_range` returned the requested blob with `Status=OK`;
`absent` means it returned an empty list. Neither removes a finding. A later
scan removes each finding it no longer observes only if the VDisk coverage
matches the scan that last observed that finding, or includes all nine VDisks.
Coverage is persisted per finding: repeated scans of a different set of VDisks
cannot close an older finding. Groups with unresolved findings use
the retry interval even when the latest scan has no candidates. The report's
`unresolved` count includes findings retained from earlier scans.
`disappeared` means no longer a candidate, not proof that a blob was copied
rather than garbage-collected.
`new_total` includes initially discovered and subsequently reappearing candidates.
Scan errors preserve existing findings. Repair failures remain visible and are
retried after another scan; there is no permanent processed-blob blacklist.

No direct writes of blob contents are performed by the service: actual repair
and GC/existence decisions remain in DSProxy through `mustRestoreFirst`.

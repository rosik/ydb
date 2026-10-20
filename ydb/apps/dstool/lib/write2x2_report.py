"""Versioned blobsan scan reports and histogram aggregation."""

import re


GC_STATES = {'hard_barrier', 'do_not_keep', 'soft_without_keep', 'soft_with_keep', 'no_covering_barrier'}
METRICS = ('blobs', 'logical_bytes', 'observed_replica_bytes', 'missing_metadata_blobs')


def bucket_key(bucket):
    return tuple(bucket[key] for key in ('local', 'ingress', 'gc'))


def totals(buckets):
    return {key: sum(bucket[key] for bucket in buckets) for key in METRICS}


def validate(report, gid, completed):
    """Reject incompatible, mismatched or internally inconsistent analyzer output."""
    if not isinstance(report, dict) or type(report.get('version')) is not int or report['version'] != 1 or report.get('group_id') != gid:
        raise ValueError('Invalid scan report version/group')
    coverage = report.get('completed_vdisks')
    if not isinstance(coverage, list) or any(type(index) is not int for index in coverage) or coverage != sorted(completed):
        raise ValueError('Scan report coverage does not match captured VDisks')
    buckets = report.get('buckets')
    if not isinstance(buckets, list):
        raise ValueError('Invalid scan report buckets')
    seen = set()
    for bucket in buckets:
        if not isinstance(bucket, dict) or not isinstance(bucket.get('local'), str) or not re.fullmatch(
                r'[0-3?]\+[0-3?]\+[0-3?]', bucket['local']):
            raise ValueError('Invalid local layout in scan report')
        if not isinstance(bucket.get('ingress'), str) or not re.fullmatch(r'[0-3]\+[0-3]\+[0-3]', bucket['ingress']):
            raise ValueError('Invalid ingress layout in scan report')
        if not isinstance(bucket.get('gc'), str) or bucket['gc'] not in GC_STATES:
            raise ValueError('Invalid GC state in scan report')
        for realm, count in enumerate(bucket['local'].split('+')):
            known = all(realm * 3 + domain in completed for domain in range(3))
            if known == (count == '?'):
                raise ValueError('Local layout contradicts scan coverage')
        if any(type(bucket.get(key)) is not int or bucket[key] < 0 for key in METRICS):
            raise ValueError('Invalid scan counters')
        if not bucket['blobs'] or bucket['missing_metadata_blobs'] > bucket['blobs']:
            raise ValueError('Invalid blob counts')
        key = bucket_key(bucket)
        if key in seen:
            raise ValueError('Duplicate scan bucket')
        seen.add(key)
    if report.get('totals') != totals(buckets):
        raise ValueError('Scan report totals do not match histogram')
    return report


def merge(histogram, buckets):
    for bucket in buckets:
        key = bucket_key(bucket)
        target = histogram.setdefault(key, dict(zip(('local', 'ingress', 'gc'), key), **dict.fromkeys(METRICS, 0)))
        for metric in METRICS:
            target[metric] += bucket[metric]


def histogram_rows(histogram):
    return [histogram[key] for key in sorted(histogram)]

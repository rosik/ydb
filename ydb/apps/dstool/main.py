#!/usr/bin/env python3

from ydb.apps.dstool.lib.arg_parser import ArgumentParser
import ydb.apps.dstool.lib.common as common
import ydb.apps.dstool.lib.commands as commands

from __res import find

import sys


def get_version():
    try:
        return find('version.txt').decode('utf-8').strip()
    except FileNotFoundError:
        return "unknown"
    except UnicodeDecodeError:
        return "unknown"


def main(args=None):
    args = list(sys.argv[1:] if args is None else args)
    # Service workers do not require a remote cluster endpoint. The master
    # and workers use the colocated viewer unless an endpoint is explicitly set.
    service = any(args[i:i + 2] == ['cluster', 'write2x2service'] for i in range(len(args) - 1))
    if service and not any(a in ('-e', '--endpoint') or a.startswith('--endpoint=') for a in args):
        args = ['-e', 'localhost'] + args
    parser = ArgumentParser(description='YDB Distributed Storage Administration Tool', version=get_version())

    # common options
    common.add_host_access_options(parser)
    parser.add_argument('--dry-run', '-n', action='store_true', help='Run command without side effects')

    subparsers = parser.add_subparsers(help='Subcommands', dest='global_command', required=True)
    command_map = commands.make_command_map_by_structure(subparsers)
    args = parser.parse_args(args)
    try:
        common.apply_args(args)
        commands.run_command(command_map, args)
    except common.InvalidParameterError as e:
        e.print()
        sys.exit(1)


if __name__ == '__main__':
    main()

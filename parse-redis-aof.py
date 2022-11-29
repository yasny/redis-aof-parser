#!/usr/bin/env python3
import sys
import argparse


def skip_rdb_preamble(rdb):
    rdb.seek(0)
    buffer = rdb.read(9)
    if buffer[0:5] != bytes('REDIS', 'utf-8'):
        raise IOError('Wrong signature')

    line_count = 0
    for line in rdb:
        line_count += 1
        # NOTE(iwalker): search for lines starting with $ or *, since that is the start of the actual AOF file
        if line[0] == ord('$') or line[0] == ord('*'):
            break

    # NOTE(iwalker): if we found a $, we need to read back until we find a *
    tries = 10
    if line[0] == ord('$'):
        rdb.seek(rdb.tell() - len(line) - 1)
        check = rdb.read(1)
        while check[0] != ord('*'):
            rdb.seek(rdb.tell() - 2)
            check = rdb.read(1)
            tries -= 1
            if tries <= 0:
                raise IOError('Error: RDB preamble does not seem to be correct.')

    pos = rdb.seek(rdb.tell() - 1)
    return pos


def parse_aof(filename, separator, max_command_count):
    with open(filename, 'rb') as f:
        signature = f.read(5)
        if signature == bytes('REDIS', 'utf-8'):
            print('INFO: Found RDB preamble!', file=sys.stderr)
            pos = skip_rdb_preamble(f)
            print(f'INFO: Skipped {pos} byte RDB preamble.', file=sys.stderr)
        else:
            f.seek(0)

        argc = 0
        processed_count = 0
        while True:
            line = f.readline().decode('utf-8')
            if not line:
                break

            if not line.startswith('*'):
                raise IOError(f'AOF file format error at byte {f.tell()}')

            argc = int(line[1:])

            if argc < 1:
                raise IOError('AOF file format error')

            command = list()
            for _ in range(argc * 2):
                cmd = f.readline().decode('utf-8')
                if cmd.startswith('$'):
                    continue
                command.append(cmd.strip('\r\n'))

            # NOTE(iwalker): script commands can contain multi-line scripts.
            # Read the script lines until we get just a newline by itself that signifies the script end.
            if command[0] == 'script':
                script_line = f.readline().decode('utf-8').strip('\r\n')
                while len(script_line) != 0:
                    command[2] += '\n' + script_line
                    script_line = f.readline().decode('utf-8').strip('\r\n')

            if command:
                print(separator.join(command))

            processed_count += 1

            if processed_count % 10000 == 0:
                print(f'Processed {processed_count} commands...', file=sys.stderr)

            if max_command_count > 0 and processed_count >= max_command_count:
                break

        if processed_count > 0:
            print(f'Processed a total of {processed_count} commands.', file=sys.stderr)


if __name__ == '__main__':
    p = argparse.ArgumentParser(description="Parse Redis AOF file", conflict_handler="error")
    p.add_argument('aoffile', type=str, help="AOF file")
    p.add_argument('-l', '--limit', type=int, metavar='COUNT', help='Limit number of commands', default=-1)
    p.add_argument('-s', dest='separator', type=str, metavar='SEPARATOR', help='Command argument separator', default='\t')

    args = p.parse_args()
    parse_aof(args.aoffile, args.separator, args.limit)

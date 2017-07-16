#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#
#   Author  :   renyuneyun
#   E-mail  :   renyuneyun@gmail.com
#   Date    :   17/07/15 12:03:52
#   License :   Apache 2.0 (See LICENSE)
#

'''

'''

from schema import engine, record
from sqlalchemy.exc import IntegrityError
import sys

conn = engine.connect()

def import_every_line(lines):
    for i, line in enumerate(lines):
        if i == 0 and line.startswith('platform '):
            continue
        if line.startswith('#'):
            continue
        parts = line.split()
        try:
            int(parts[0])
            start = 0
            platform = ''
        except:
            start = 1
            platform = parts[0]
        num_of_iter, np, num_of_sieve, max_prime, mpi_time, mpi_inc_time = parts[start:]
        num_of_iter, np, num_of_sieve, max_prime, mpi_time, mpi_inc_time = int(num_of_iter), int(np), int(num_of_sieve), int(max_prime), float(mpi_time), float(mpi_inc_time)
        ins = record.insert().values(platform=platform, num_iter=num_of_iter, np_mpi_inc=np, max_num_sieve=num_of_sieve, max_prime=max_prime, mpi_time=mpi_time, mpi_inc_time=mpi_inc_time)
        try:
            result = conn.execute(ins)
        except IntegrityError:
            print("Data exists")

if len(sys.argv) == 2:
    with open(sys.argv[1]) as fd:
        import_every_line(fd)
else:
    import_every_line(sys.stdin)


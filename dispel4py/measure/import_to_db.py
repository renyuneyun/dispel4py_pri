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
        is_outlier = False
        if line.startswith('#'):
            is_outlier = True
            line = line[1:]
        parts = line.split()
        platform = ''
        varsion = ''
        run_id = ''
        try:
            num_of_iter, np, num_of_sieve, max_prime, mpi_time, mpi_inc_time = parts
        except ValueError:
            try:
                platform, num_of_iter, np, num_of_sieve, max_prime, mpi_time, mpi_inc_time = parts
                if platform.endswith('_opt1'):
                    platform = platform[:-5]
                    version = 'opt1'
            except ValueError:
                platform, version, run_id, num_of_iter, np, num_of_sieve, max_prime, mpi_time, mpi_inc_time = parts
        num_of_iter, np, num_of_sieve, max_prime, mpi_time, mpi_inc_time = int(num_of_iter), int(np), int(num_of_sieve), int(max_prime), float(mpi_time), float(mpi_inc_time)
        ins1 = record.insert().values(outlier=is_outlier, platform=platform, version=version, run_id=run_id, module='mpi', num_iter=num_of_iter, np=num_of_sieve+1, max_num_sieve=num_of_sieve, max_prime=max_prime, time=mpi_time)
        ins2 = record.insert().values(outlier=is_outlier, platform=platform, version=version, run_id=run_id, module='mpi_inc', num_iter=num_of_iter, np=np, max_num_sieve=num_of_sieve, max_prime=max_prime, time=mpi_inc_time)
        try:
            result = conn.execute(ins1)
        except IntegrityError:
            print("Data exists")
        try:
            result = conn.execute(ins2)
        except IntegrityError:
            print("Data exists")

if len(sys.argv) == 2:
    with open(sys.argv[1]) as fd:
        import_every_line(fd)
else:
    import_every_line(sys.stdin)


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

def insert_to_db(ins):
    try:
        result = conn.execute(ins)
        return result
    except IntegrityError:
        print("Data exists")

def import_every_line(lines):
    for i, line in enumerate(lines):
        if i == 0:
            if line.startswith('platform ') or line.startswith('workflow'):
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
            ins1 = record.insert().values(outlier=is_outlier, workflow='repeatable_prime_sieve__static', platform=platform, version=version, run_id=run_id, module='mpi', num_iter=num_of_iter, np=num_of_sieve+1, max_num_sieve=num_of_sieve, max_prime=max_prime, time=mpi_time)
            ins2 = record.insert().values(outlier=is_outlier, workflow='repeatable_prime_sieve', platform=platform, version=version, run_id=run_id, module='mpi_inc', num_iter=num_of_iter, np=np, max_num_sieve=num_of_sieve, max_prime=max_prime, time=mpi_inc_time)
            insert_to_db(ins1)
            insert_to_db(ins2)
        except ValueError:
            workflow, platform, version, run_id, module, num_of_iter, np, num_of_sieve, max_prime, exec_time = parts
            num_of_iter, np, num_of_sieve, max_prime, exec_time = int(num_of_iter), int(np), int(num_of_sieve), int(max_prime), float(exec_time)
            ins = record.insert().values(outlier=is_outlier, workflow=workflow, platform=platform, version=version, run_id=run_id, module=module, num_iter=num_of_iter, np=np, max_num_sieve=num_of_sieve, max_prime=max_prime, time=exec_time)
            insert_to_db(ins)

if len(sys.argv) == 2:
    with open(sys.argv[1]) as fd:
        import_every_line(fd)
else:
    import_every_line(sys.stdin)


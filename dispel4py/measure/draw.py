#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#
#   Author  :   renyuneyun
#   E-mail  :   renyuneyun@gmail.com
#   Date    :   17/07/10 10:12:36
#   License :   Apache 2.0 (See LICENSE)
#

'''

'''

import matplotlib.pyplot as plt
import numpy as np

from schema import engine, record
from sqlalchemy.sql import select

conn = engine.connect()

combine_different_runs = True

def all_platforms():
    s = select([record.c.platform, record.c.version, record.c.run_id]).distinct()
    results = conn.execute(s)
    for line in results:
        yield line[0]

def all_confs_of_platform(platform):
    targets = [record.c.max_num_sieve, record.c.max_prime]
    s = select(targets) \
            .where(record.c.platform==platform[0]).where(record.c.version==platform[1]).where(record.c.run_id==platform[2]) \
            .distinct()
    results = conn.execute(s)
    for line in results:
        yield line

def all_confs():
    targets = [record.c.max_num_sieve, record.c.max_prime]
    s = select(targets).distinct().order_by(record.c.max_num_sieve)
    results = conn.execute(s)
    for line in results:
        yield line

def all_platforms_of_conf(conf):
    targets = [record.c.platform, record.c.version]
    if not combine_different_runs:
        targets.append(record.c.run_id)
    s = select(targets).where(record.c.max_num_sieve==conf[0]).where(record.c.max_prime==conf[1]).distinct()
    results = conn.execute(s)
    for line in results:
        yield line

def all_time_of_conf(platform, conf):
    def get_mpi():
        mpi_times = {}
        s = select([record.c.num_iter, record.c.np, record.c.time]) \
                .where(record.c.outlier==False).where(record.c.module=='mpi') \
                .where(record.c.platform==platform[0])
        if not combine_different_runs:
            s = s.where(record.c.run_id==platform[2])
        s = s.where(record.c.max_num_sieve==conf[0]).where(record.c.max_prime==conf[1]) \
                .order_by(record.c.np, record.c.num_iter)
        results = conn.execute(s)
        for line in results:
            num_iter = line[0]
            num_p = line[1]
            time = line[2]
            if num_p not in mpi_times:
                mpi_times[num_p] = ([], [])
            con = mpi_times[num_p]
            if con[0] and con[0][-1] == num_iter:
                con[1][-1].append(time)
            else:
                con[0].append(num_iter)
                con[1].append([time])
        return mpi_times
    def get_mpi_inc():
        mpi_inc_times = {}
        s = select([record.c.num_iter, record.c.np, record.c.time]) \
                .where(record.c.outlier==False).where(record.c.module=='mpi_inc') \
                .where(record.c.platform==platform[0]).where(record.c.version==platform[1])
        if not combine_different_runs:
            s = s.where(record.c.run_id==platform[2])
        s = s.where(record.c.max_num_sieve==conf[0]).where(record.c.max_prime==conf[1]) \
                .order_by(record.c.np, record.c.num_iter)
        results = conn.execute(s)
        for line in results:
            num_iter = line[0]
            num_p = line[1]
            time = line[2]
            if num_p not in mpi_inc_times:
                mpi_inc_times[num_p] = ([], [])
            con = mpi_inc_times[num_p]
            if con[0] and con[0][-1] == num_iter:
                con[1][-1].append(time)
            else:
                con[0].append(num_iter)
                con[1].append([time])
        return mpi_inc_times
    mpi_times, mpi_inc_times = get_mpi(), get_mpi_inc()
    return mpi_times, mpi_inc_times

avg = lambda lst: sum(lst) / len(lst)
flatten = lambda l: [item for sub in l for item in sub]
expand = lambda l1, l2: flatten([[item] * len(l2[i]) for i, item in enumerate(l1)])

capsize = 5

confs = list(all_confs())
fig, axes = plt.subplots(len(confs), sharex=True)

fig.text(0.5, 0.04, 'number of iterations', ha='center')
fig.text(0.04, 0.5, 'time', va='center', rotation='vertical')

for i, conf in enumerate(confs):
    subplot = axes[i]
    subplot.set_title("max_num_sieve:{} max_prime:{}".format(conf[0], conf[1]))
    for platform in all_platforms_of_conf(conf):
        platform_str = str(platform[0])
        if platform[1]:
            platform_str += "-{}".format(platform[1])
            if not combine_different_runs and platform[2]:
                platform_str += "-{}".format(platform[2])
        else:
            if not combine_different_runs and platform[2]:
                platform_str += "--{}".format(platform[2])
        mpi_times, mpi_inc_times = all_time_of_conf(platform, conf)
        for num_p, (num_iters, times) in mpi_times.items():
            label_old = "old {} np:{}".format(platform_str, num_p)
            p = subplot.errorbar(num_iters, list(map(avg, times)), yerr=list(map(np.std, times)), capsize=capsize, linestyle='dashed', label=label_old)
            color = p[0].get_color()
            subplot.plot(list(expand(num_iters, times)), list(flatten(times)), '.', color=color)
        for num_p, (num_iters, times) in mpi_inc_times.items():
            label_mine = "mine {} np:{}".format(platform_str, num_p)
            p = subplot.errorbar(num_iters, list(map(avg, times)), yerr=list(map(np.std, times)), capsize=capsize, label=label_mine)
            color = p[0].get_color()
            subplot.plot(list(expand(num_iters, times)), list(flatten(times)), 'x', color=color)
    subplot.legend()

plt.suptitle('Execution time on different platforms under different configurations')
plt.show()


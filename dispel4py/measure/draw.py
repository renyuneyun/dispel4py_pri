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

def R(name):
    return getattr(record.c, name)

def R_(names):
    return list(map(R, names))

def pure(pairs, order=[]):
    if order:
        return [pairs[name] for name in order]
    else:
        return pairs.values()

def all_of(columns, where={}, distinct=False, order_by=[]):
    targets = R_(columns)
    s = select(targets)
    for k, v in where.items():
        s = s.where(R(k) == v)
    if distinct:
        s = s.distinct()
    if order_by:
        s = s.order_by(*R_(order_by))
    results = conn.execute(s)
    for line in results:
        yield dict(zip(columns, line))

def get_times(where):
    targets = ['num_iter', 'np', 'time']
    order_by = ['np', 'num_iter']
    times = {}
    for line in all_of(targets, where, order_by=order_by):
        num_iter = line['num_iter']
        num_p = line['np']
        time = line['time']
        if num_p not in times:
            times[num_p] = ([], [])
        con = times[num_p]
        if con[0] and con[0][-1] == num_iter:
            con[1][-1].append(time)
        else:
            con[0].append(num_iter)
            con[1].append([time])
    return times

def get_mpi(platform, conf):
    where = {**platform, **conf}
    where['outlier'] = False
    where['module'] = 'mpi'
    return get_times(where)

def get_mpi_inc(platform, conf):
    where = {**platform, **conf}
    where['outlier'] = False
    where['module'] = 'mpi_inc'
    return get_times(where)

avg = lambda lst: sum(lst) / len(lst)
flatten = lambda l: [item for sub in l for item in sub]
expand = lambda l1, l2: flatten([[item] * len(l2[i]) for i, item in enumerate(l1)])

capsize = 5

conf_cols = ['max_num_sieve', 'max_prime']

platform_columns_mpi = ['platform'] + ([] if combine_different_runs else ['run_id'])

def key_mpi_platform(platform):
    return tuple(platform[name] for name in platform_columns_mpi)

mpi_times = {tuple(pure(conf, conf_cols)):
        {key_mpi_platform(platform):
            get_mpi(platform, conf)
            for platform in all_of(platform_columns_mpi, where=conf, distinct=True)}
        for conf in all_of(conf_cols, where={'module':'mpi'}, distinct=True, order_by=['max_num_sieve'])}

confs_mpi_inc = list(all_of(conf_cols, where={'module':'mpi_inc'}, distinct=True, order_by=['max_num_sieve']))
fig, axes = plt.subplots(len(confs_mpi_inc), sharex=True)

fig.text(0.5, 0.04, 'number of iterations', ha='center')
fig.text(0.04, 0.5, 'time', va='center', rotation='vertical')

drawn = {}
for i, conf in enumerate(confs_mpi_inc):
    subplot = axes[i]
    subplot.set_title("max_num_sieve:{} max_prime:{}".format(conf['max_num_sieve'], conf['max_prime']))
    k_conf = tuple(pure(conf, conf_cols))
    drawn[k_conf] = {}
    for platform in all_of(['platform', 'version'] + ([] if combine_different_runs else ['run_id']), where=conf, distinct=True):
        k_platform = key_mpi_platform(platform)
        if k_platform not in drawn[k_conf]:
            try:
                platform_str = str(platform['platform'])
                if not combine_different_runs and platform['run_id']:
                    platform_str += "-{}".format(platform['run_id'])
                for num_p, (num_iters, times) in mpi_times[k_conf][k_platform].items():
                    label_old = "old {} np:{}".format(platform_str, num_p)
                    p = subplot.errorbar(num_iters, list(map(avg, times)), yerr=list(map(np.std, times)), capsize=capsize, linestyle='dashed', label=label_old)
                    color = p[0].get_color()
                    subplot.plot(list(expand(num_iters, times)), list(flatten(times)), '.', color=color)
                drawn[k_conf][k_platform] = True
            except KeyError:
                drawn[k_conf][k_platform] = False
        platform_str = str(platform['platform'])
        if platform['version']:
            platform_str += "-{}".format(platform['version'])
            if not combine_different_runs and platform['run_id']:
                platform_str += "-{}".format(platform['run_id'])
        else:
            if not combine_different_runs and platform['run_id']:
                platform_str += "--{}".format(platform['run_id'])
        mpi_inc_times = get_mpi_inc(platform, conf)
        for num_p, (num_iters, times) in mpi_inc_times.items():
            label_mine = "mine {} np:{}".format(platform_str, num_p)
            p = subplot.errorbar(num_iters, list(map(avg, times)), yerr=list(map(np.std, times)), capsize=capsize, label=label_mine)
            color = p[0].get_color()
            subplot.plot(list(expand(num_iters, times)), list(flatten(times)), 'x', color=color)
    subplot.legend()

plt.suptitle('Execution time on different platforms under different configurations')
plt.show()


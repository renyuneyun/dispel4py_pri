#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#
#   Author  :   renyuneyun
#   E-mail  :   renyuneyun@gmail.com
#   Date    :   17/08/09 11:34:20
#   License :   Apache 2.0 (See LICENSE)
#

'''

'''

import sys

max_num = int(sys.argv[1])

sieves_in_each_partition = int(sys.argv[2]) if len(sys.argv) >= 2 else 10

num_prime = 0

with open('prime_list') as fd:
    for line in fd:
        cur_prime = int(line)
        if cur_prime > max_num:
            break
        num_prime += 1

code_static = """
from dispel4py.workflow_graph import WorkflowGraph

from dispel4py.examples.graph_testing.testing_PEs\\
    import IntegerProducer, RepeatablePrimeSieve

sieves = [[]]

producer = IntegerProducer(2, {})
sieve = RepeatablePrimeSieve()
graph = WorkflowGraph()
graph.connect(producer, 'output', sieve, RepeatablePrimeSieve.INPUT_NUMBER_LINE)

prev = sieve
for i in range(1, {}):
    sieves[-1].append(sieve)
    if len(sieves[-1]) == {}:
        sieves.append([])
    sieve = RepeatablePrimeSieve()
    graph.connect(prev, RepeatablePrimeSieve.OUTPUT_NUMBER_LINE, sieve, RepeatablePrimeSieve.INPUT_NUMBER_LINE)
    prev = sieve
sieves[-1].append(sieve)

graph.partitions = [[producer], *sieves]
"""

with open("repeatable_prime_sieve__partition_{}__static_{}.py".format(sieves_in_each_partition, num_prime), 'w') as fd:
    fd.write(code_static.format(max_num, num_prime, sieves_in_each_partition))

#code_dynamic = """
#from dispel4py.workflow_graph import WorkflowGraph
#
#from dispel4py.examples.graph_testing.testing_PEs\
#    import IntegerProducer, RepeatablePrimeSieve
#
#producer = IntegerProducer(2, {})
#sieve = RepeatablePrimeSieve()
#graph = WorkflowGraph()
#graph.connect(producer, 'output', sieve, RepeatablePrimeSieve.INPUT_NUMBER_LINE)
#"""
#
#with open("repeatable_prime_sieve_{}.py".format(max_num), 'w') as fd:
#    fd.write(code_dynamic.format(max_num))


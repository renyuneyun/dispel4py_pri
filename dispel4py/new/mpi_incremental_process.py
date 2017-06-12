# Copyright (c) The University of Edinburgh 2014-2015
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

'''
Enactment of dispel4py graphs with MPI.

From the commandline, run the following command::

    dispel4py mpi <module> [-h] [-a attribute] [-f inputfile] [-i iterations]

with parameters

:module:    module that creates a Dispel4Py graph
:-a attr:   name of the graph attribute within the module (optional)
:-f file:   file containing input data in JSON format (optional)
:-i iter:   number of iterations to compute (default is 1)
:-h:        print this help page

For example::

    mpiexec -n 6 dispel4py mpi dispel4py.examples.graph_testing.pipeline_test\
        -i 5
    Processing 5 iterations.
    Processing 5 iterations.
    Processing 5 iterations.
    Processing 5 iterations.
    Processing 5 iterations.
    Processing 5 iterations.
    Processes: {'TestProducer0': [5], 'TestOneInOneOut5': [2],\
        'TestOneInOneOut4': [4], 'TestOneInOneOut3': [3],\
        'TestOneInOneOut2': [1], 'TestOneInOneOut1': [0]}
    TestOneInOneOut1 (rank 0): Processed 5 iterations.
    TestOneInOneOut2 (rank 1): Processed 5 iterations.
    TestOneInOneOut3 (rank 3): Processed 5 iterations.
    TestProducer0 (rank 5): Processed 5 iterations.
    TestOneInOneOut4 (rank 4): Processed 5 iterations.
    TestOneInOneOut5 (rank 2): Processed 5 iterations.
'''

from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

from dispel4py.new.processor\
    import GenericWrapper, simpleLogger, STATUS_TERMINATED, STATUS_ACTIVE
from dispel4py.new import processor

import argparse
import sys
import types
import traceback


from typing import Dict, Iterable, List, Tuple, Union
from dispel4py.new.processor import IOMapping, Partition
from dispel4py.workflow_graph import WorkflowNode, WorkflowGraph
from dispel4py.core import GenericPE, GROUPING

DEBUG=-1
def debug_fn(level:int):
    def fn(msg: str):
        if DEBUG >= level:
            print(msg)
    return fn
dbg0 = debug_fn(0) #node executing state (excuted or not; what it is)
dbg1 = debug_fn(1) #communications between nodes and vital function calling/returning
dbg2 = debug_fn(2) #main functions called/returing and main data
dbg3 = debug_fn(3) #secondary data
dbg4 = debug_fn(4) #detailed data

def mpi_excepthook(type, value, trace):
    '''
    Sending abort to all processes if an exception is raised.
    '''
    #if rank == 0:
    print("###Exception encountered at rank {}###".format(rank))
    print("Type:", type)
    print("Value:", value)
    traceback.print_tb(trace)
    comm.Abort(1)


sys.excepthook = mpi_excepthook


def parse_args(args, namespace):
    parser = argparse.ArgumentParser(
        description='Submit a dispel4py graph to MPI processes.')
    parser.add_argument('-s', '--simple', help='force simple processing',
                        action='store_true')
    result = parser.parse_args(args, namespace)
    return result

TAG_DEPLOY = 50
TAG_REQUIRE = 51
TAG_WAIT = 53
TAG_TARGET = 52
TAG_BROTHER = 62
TAG_INPUT_MAPPING = 60
TAG_OUTPUT_MAPPING = 61
TAG_DATA = 0
TAG_FINALIZE = 100

RANK_COORDINATOR=0

def coordinator(workflow: WorkflowGraph, inputs, args):
    def getWorkflowProperty(workflow: WorkflowGraph) -> Tuple[List[GenericPE], int]:
        def initial_nodes(workflow: WorkflowGraph) -> List[GenericPE]:
            graph = workflow.graph
            all_initial_nodes = []
            for node in graph.nodes():
                is_initial = True
                for edge in graph.edges(node, data=True):
                    direction = edge[2]['DIRECTION']
                    dest = direction[1]
                    if dest == node.getContainedObject():
                        is_initial = False
                        break
                if is_initial:
                    all_initial_nodes.append(node.getContainedObject())
            return all_initial_nodes
        all_initial_nodes = initial_nodes(workflow)
        totalProcesses = sum((pe.numprocesses if pe not in all_initial_nodes else 1 for pe in (wfNode.getContainedObject() for wfNode in workflow.graph.nodes())))
        return all_initial_nodes, totalProcesses

    class TaskList:
        def __init__(self, size: int, numSources: int=-1, totalProcesses: int=-1):
            self.task_list = [None] * size
            self.task_list[0] = 0 # special mark for coordinator
            self.max_used_nodes = 0
            self.numSources = numSources
            self.totalProcesses = totalProcesses
        @property
        def size(self):
            return len(self.task_list) - 1
        def find_assignable(self, numproc=1, is_source=False) -> List[int]:
            assignables = []
            if is_source:
                prcs = 1
            elif self.numSources > 0 and self.totalProcesses > 0:
                prcs = processor._getNumProcesses(self.size, self.numSources, numproc, self.totalProcesses)
            else:
                prcs = numproc
            #prcs = 1
            for i, pe in enumerate(self.task_list):
                if i == 0: continue
                if pe == None:
                    assignables.append(i)
                    if len(assignables) == prcs:
                        return assignables
            dbg0("no enough assignable processes [expected:{} needed:{} free:{}]".format(numproc, prcs, len(assignables)))
            raise Exception("shouldn't run out of nodes")
        def assign(self, index: int, pe: GenericPE):
            self.task_list[index] = pe
            dbg2("[TaskList] [assign] {}:{} -> {}".format(index, pe, self.task_list))
            self.max_used_nodes = max(self.max_used_nodes, len(self.working_nodes()))
        def working_nodes(self) -> List[int]: # May be replace with "num_working_nodes" because nowhere uses the actual nodes
            working = []
            for i, pe in enumerate(self.task_list):
                if i == 0: continue
                if pe != None:
                    working.append(i)
            dbg4("[TaskList] working_nodes: {}".format(working))
            return working
        def lookup(self, target_pe: GenericPE) -> List[int]:
            matches = []
            for i, pe in enumerate(self.task_list):
                if i == 0: continue
                if pe == target_pe:
                    matches.append(i)
            return matches
        def get_node(self, index: int) -> GenericPE:
            return self.task_list[index]
        def remove(self, index: int):
            self.task_list[index] = None
            dbg2("TaskList after removing: {}".format(self.task_list))

    def assign_node(workflow: WorkflowGraph, pe: GenericPE, task_list: TaskList, is_source=True) -> List[int]:
        dbg2("[assign_node]")
        target_ranks = task_list.find_assignable(pe.numprocesses, is_source=is_source)
        dbg3("[assign_node] target_ranks:{}".format(target_ranks))
        for target_rank in target_ranks:
            dbg1("[assign_node] assigning:{}".format(target_rank))
            comm.send(pe.id, target_rank, tag=TAG_DEPLOY) #Is pe.id reliable for different processes (original mpi version assumes this)?
            dbg1("[assign_node] assigning:{} pe sent:{}".format(target_rank, pe.id))
            comm.send(target_ranks, target_rank, tag=TAG_BROTHER)
            dbg1("[assign_node] assigning:{} brothers sent:{}".format(target_rank, target_ranks))
            task_list.assign(target_rank, pe)
            dbg3("[assign_node] assigning:{} assignment recorded".format(target_rank))
        dbg2("[assign_node] finishing")
        return target_ranks

    def onRequire(output_name: str, source_rank: int, workflow: WorkflowGraph, task_list: TaskList):
        dbg1("[coordinator] onRequire")
        source_pe = task_list.get_node(source_rank)
        dbg2("[coordinator] source_pe: {}".format(source_pe))
        source_wfNode = workflow.objToNode[source_pe]
        dbg4("[coordinator] source_wfNode: {}".format(source_wfNode))
        target_wfNodes = []
        for (linked_wfNode, attributes) in workflow.graph[source_wfNode].items():
            if attributes['DIRECTION'][0] == source_pe:
                for fromConnection, toConnection in attributes['ALL_CONNECTIONS']:
                    if fromConnection == output_name:
                        target_wfNodes.append((toConnection, linked_wfNode))
                        dbg4("[coordinator] target_wfNode found: {}".format(linked_wfNode))
        if not target_wfNodes:
            dbg1("[coordinator] encountered finial node of stream: {} [{}]".format(output_name, source_rank))
            comm.send([], source_rank, tag=TAG_TARGET)
            dbg1("[coordinator] targets:{} sent to {}".format([], source_rank))
            return
        dbg3("[coordinator] looping wfNodes: {}".format(target_wfNodes))
        all_indices = {}
        for (input_name, required_pe) in ((toConnection, wfNode.getContainedObject()) for (toConnection, wfNode) in target_wfNodes):
            dbg3("[coordinator] required_pe: {}".format(required_pe))
            dbg4("[coordinator] looking up if exists")
            indices = task_list.lookup(required_pe)
            dbg2("[coordinator] lookup finished: {}".format(indices))
            if len(indices) == 0:
                dbg2("[coordinator] no existing")
                indices = assign_node(workflow, required_pe, task_list, is_source=False)
                dbg3("[coordinator] new nodes assigned: {}".format(indices))
            all_indices[(input_name, required_pe.id)] = indices
        dbg1("[coordinator] sending targets to {}".format(source_rank))
        comm.send(all_indices, source_rank, tag=TAG_TARGET)
        dbg1("[coordinator] targets sent: {}".format(indices))

    dbg0("[coordinator]")
    dbg3("[coordinator] initialising")
    initial_nodes, numProcesses = getWorkflowProperty(workflow)
    task_list = TaskList(size, len(initial_nodes), numProcesses)
    dbg2("[coordinator] going to deploy initial nodes")
    for node in initial_nodes:
        assign_node(workflow, node, task_list, is_source=True)
    dbg2("[coordinator] initial nodes deployed")

    status = MPI.Status()
    while True:
        dbg0("[coordinator] waiting for request")
        comm.probe(status=status)
        tag = status.Get_tag()
        msg = comm.recv(tag=tag, status=status)
        source_rank = status.Get_source()
        dbg1("[coordinator] request got: {} [from:{}]".format(msg, source_rank))
        if tag == TAG_REQUIRE:
            onRequire(msg, source_rank, workflow, task_list)
        elif tag == STATUS_TERMINATED:
            dbg0("[coordinator] onFinish")
            task_list.remove(source_rank)
            if not task_list.working_nodes(): #Because we know MPI guarentees FIFO for each pair's communication, we can safely say there is no request on-the-fly
                dbg1("[coordinator] sending finalize communication to all nodes")
                for i in range(1, size):
                    comm.isend(None, i, tag=TAG_FINALIZE)
                dbg1("[coordinator] finalize communication sent")
                break
        else:
            dbg0("[coordinator] unexpected tag: {} [from:{}]".format(tag, source_rank))
            raise Exception("unexpected tag")
    dbg0("coordinator exit [max_used_nodes: {} (coordinator excluded)]".format(task_list.max_used_nodes))



def executor(workflow, inputs, args):
    dbg0("[executor {}]".format(rank))
    status = MPI.Status()
    id_to_pe = {pe.id:pe for pe in (wfNode.getContainedObject() for wfNode in workflow.graph.nodes())}
    while True:
        dbg0("[executor {}] waiting for communication from coordinator".format(rank))
        comm.probe(source=RANK_COORDINATOR, status=status)
        tag = status.Get_tag()
        dbg1("[executor {}] communication got: tag:{}".format(rank, tag))
        if tag == TAG_DEPLOY:
            pe_id = comm.recv(source=RANK_COORDINATOR, tag=TAG_DEPLOY)
            pe = id_to_pe[pe_id]
            dbg1("[executor {}] node received: {}".format(rank, pe.id))
            brothers = comm.recv(source=RANK_COORDINATOR, tag=TAG_BROTHER) #nodes executing the same pe (same node in the workflow graph)
            dbg1("[executor {}] brothers received: {}".format(rank, brothers))
            dbg4("[executor {}] going to get_inputs".format(rank))
            provided_inputs = processor.get_inputs(pe, inputs)
            dbg4("[executor {}] finished get_inputs: {}".format(rank, provided_inputs))
            wrapper = MPIIncWrapper(workflow, pe, brothers=brothers, provided_inputs=provided_inputs)
            dbg0("[executor {}] finished creating wrapper - executing".format(rank))
            wrapper.process()
        elif tag == TAG_FINALIZE:
            break
    dbg0("[executor {}] finishing".format(rank))

def process(workflow, inputs, args):
    if rank == 0:
        print([(edge[0].getContainedObject().id,edge[1].getContainedObject().id) for edge in workflow.graph.edges()])
    if rank == 0:
        coordinator(workflow, inputs, args)
    else:
        executor(workflow, inputs, args)


class MPIIncWrapper(GenericWrapper):

    def __init__(self, workflow: WorkflowGraph, pe: GenericPE, brothers=[], provided_inputs=None):
        GenericWrapper.__init__(self, pe)
        self.workflow = workflow
        self.pe.log = types.MethodType(simpleLogger, pe)
        self.pe.rank = rank
        self.brothers = brothers
        self.rep = brothers[0]
        self.provided_inputs = provided_inputs
        self.terminated = 0
        self._num_sources = len(list(self.workflow.inputEdges(pe)))
        self.targets = {}
        self.pending_messages = {}
        self.fd = open("outputs/mpi_inc/{}".format(pe.id), 'a')

    def is_rep(self):
        return self.rep == rank

    def request_target(self, target: str):
        dbg1("[{}] request_target: {}".format(rank, target))
        comm.send(target, RANK_COORDINATOR, tag=TAG_REQUIRE)
        dbg1("[{}] request sent".format(rank))
        dbg1("[{}] waiting for replies".format(rank))
        target_ranks_list = comm.recv(source=RANK_COORDINATOR, tag=TAG_TARGET)
        dbg1("[{}] [reply] target_ranks_list: {}".format(rank, target_ranks_list))
        if not target_ranks_list:
            return []
        dbg2("[{}] request_target finished".format(rank))
        return target_ranks_list

    def create_communication_for_output(self, output_name: str):
        dbg1("[{}] creating communication for output: {}".format(rank, output_name))
        target_ranks_list = self.request_target(output_name)
        if target_ranks_list:
            dbg2("[{}] creating communications".format(rank))
            for target_pe, allconnections in self.workflow.outputConnections(self.pe):
                dbg3("[{}] target_pe:{} allconnections: {}".format(rank, target_pe, allconnections))
                for (source_output, dest_input) in allconnections:
                    if source_output == output_name:
                        dbg4("[{}] found dest_input: {}".format(rank, dest_input))
                        target_ranks = target_ranks_list[(dest_input, target_pe.id)]
                        dbg4("[{}] target_ranks: {}".format(rank, target_ranks))
                        try:
                            groupingtype = target_pe.inputconnections[dest_input][GROUPING]
                        except KeyError:
                            groupingtype = None
                        dbg4("[{}] groupingtype: {}".format(rank, groupingtype))
                        communication = processor._getCommunication(self.brothers.index(rank), dest_input, target_ranks, groupingtype=groupingtype)
                        if output_name not in self.targets:
                            self.targets[output_name] = []
                        self.targets[output_name].append((dest_input, communication))
                        dbg2("[{}] created communication for targets: {} {} {}".format(rank, dest_input, target_ranks, communication))
            dbg1("[{}] created communication for output: {} {}".format(rank, output_name, self.targets[output_name]))
        else:
            self.targets[output_name] = []
            dbg1("[{}] no targets, create dummy communication".format(rank))

    def _read(self):
        dbg1("[{}] _read".format(rank))
        result = super(MPIIncWrapper, self)._read()
        if result is not None:
            dbg1("[{}] _read returning (with provided_inputs)".format(rank))
            return result

        status = MPI.Status()
        msg = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
        source, tag = status.Get_source(), status.Get_tag()
        dbg2("[{}] message got: {} with tag {} from {}".format(rank, msg, tag, source))
        while tag == STATUS_TERMINATED:
            if source in self.brothers:
                assert self.is_rep()
                assert source != rank
                self.brothers.remove(source)
            else:
                self.terminated += 1
                if self.terminated >= self._num_sources:
                    break
            msg = comm.recv(source=MPI.ANY_SOURCE,
                            tag=MPI.ANY_TAG,
                            status=status)
            source, tag = status.Get_source(), status.Get_tag()
            dbg2("[{}] message got: {} with tag {} from {}".format(rank, msg, tag, source))
        dbg1("[{}] _read returning: {} (tag:{})".format(rank, msg, tag))
        return msg, tag

    def _write(self, name, data):
        dbg1("[{}] _write name:{} data:{}".format(rank, name, data))
        if not self.pe.inputconnections:
            self.fd.write('[{}] {}\n'.format(name, data))
        if name not in self.targets:
            dbg1("[{}] target not existing".format(rank))
            self.create_communication_for_output(name)
        if not self.targets[name]:  # coordinator replied no targets
            self.pe.log('Produced output: %s' % {name: data})
            self.fd.write('[{}] {}\n'.format(name, data))
            dbg1("[{}] _write returning (no targets)".format(rank))
            return
        targets = self.targets[name]
        dbg3("[{}] targets got: {}".format(rank, targets))
        for (inputName, communication) in targets:
            dbg3("[{}] communication:{}".format(rank, communication))
            output = {inputName: data}
            dest = communication.getDestination(output)
            dbg4("[{}] name:{} data:{} dest:{}".format(rank, name, data, dest))
            for i in dest:
                try:
                    # self.pe.log('Sending %s to %s' % (output, i))
                    dbg1("[{}] sending {} to {}".format(rank, output, i))
                    request = comm.issend(output, tag=STATUS_ACTIVE, dest=i)
                    req_key = (name, inputName, i)
                    if req_key in self.pending_messages:
                        self.pending_messages[req_key].Free()
                    self.pending_messages[req_key] = request
                    dbg1("[{}] data sent".format(rank))
                except:
                    self.pe.log(
                        'Failed to send data stream "%s" to rank %s: %s'
                        % (name, i, traceback.format_exc()))
        dbg1("[{}] _write returning".format(rank))

    def _terminate(self):
        dbg1("[{}] _terminate {}".format(rank, 'is rep' if self.is_rep() else 'is not rep'))
        if self.is_rep():
            status = MPI.Status()
            dbg2("[{}] remaining brothers {}".format(rank, self.brothers))
            while self.brothers[1:]:
                msg = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
                source, tag = status.Get_source(), status.Get_tag()
                dbg1("[{}] message: {} with tag {} from {}".format(rank, msg, tag, source))
                dbg4("[{}] asserting tag:{} is {}".format(rank, tag, STATUS_TERMINATED))
                assert tag == STATUS_TERMINATED
                dbg4("[{}] asserting source:{} in {}".format(rank, source, self.brothers))
                assert source in self.brothers
                dbg4("[{}] asserting msg:{} is {}".format(rank, msg, None))
                assert not msg
                self.brothers.remove(source)

            for output in self.pe.outputconnections:
                if output not in self.targets:
                    self.create_communication_for_output(output)
                targets = self.targets[output]
                for (inputName, communication) in targets:
                    for i in communication.destinations:
                        # self.pe.log('Terminating consumer %s' % i)
                        comm.isend(None, tag=STATUS_TERMINATED, dest=i)
        else:
            dbg1("[{}] waiting for data to be received by all targets {}".format(rank, self.pending_messages))
            MPI.Request.Waitall(list(self.pending_messages.values()))
            dbg1("[{}] data received by all targets".format(rank))
            #for req in self.pending_messages.values():
            #    req.Free()
            dbg1("[{}] sending terminate to rep".format(rank))
            comm.send(None, self.rep, tag=STATUS_TERMINATED)
            dbg1("[{}] terminate sent to rep".format(rank))
        dbg1("[{}] sending terminate to coordinator".format(rank))
        comm.send(None, RANK_COORDINATOR, tag=STATUS_TERMINATED)
        dbg1("[{}] terminate sent to coordinator".format(rank))
        dbg1("[{}] _terminate returning".format(rank))


def main():
    from dispel4py.new.processor \
        import load_graph_and_inputs, parse_common_args

    args, remaining = parse_common_args()
    try:
        args = parse_args(remaining, args)
    except SystemExit:
        raise

    graph, inputs = load_graph_and_inputs(args)
    if graph is not None:
        errormsg = process(graph, inputs, args)
        if errormsg:
            print(errormsg)

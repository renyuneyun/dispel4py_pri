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

DEBUG=1
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
    if rank == 0:
        print(type, value)
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
            #elif self.numSources > 0 and self.totalProcesses > 0:
            #    prcs = processor._getNumProcesses(self.size, self.numSources, numproc, self.totalProcesses)
            else:
                prcs = numproc
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

    def setup_outputmappings(source_pe: GenericPE, dest_pe: GenericPE, dest_processes: List[int]) -> IOMapping:
        dbg2("[coordinator] setup_outputmappings")
        source_processes = task_list.lookup(source_pe)
        allconnections = workflow.graph[workflow.objToNode[source_pe]][workflow.objToNode[dest_pe]]['ALL_CONNECTIONS']
        outputmappings = {i:{} for i in source_processes}
        dbg2("[coordinator] [setup_outputmappings] inited (source_processes:{} dest:{} allconnections:{})".format(source_processes, dest_pe, allconnections))
        for i in source_processes:
            dbg3("[coordinator] [setup_outputmappings] i={}".format(i))
            for (source_output, dest_input) in allconnections:
                dbg3("[coordinator] [setup_outputmappings] source_output:{} dest_input:{}".format(source_output, dest_input))
                communication = processor._getCommunication(
                    i, source_processes, dest_pe, dest_input, dest_processes)
                dbg4("[coordinator] [setup_outputmappings] communication: {}".format(communication))
                try:
                    outputmappings[i][source_output].append(
                        (dest_input, communication))
                except KeyError:
                    outputmappings[i][source_output] = \
                        [(dest_input, communication)]
                dbg3("[coordinator] [setup_outputmappings] outputmapping updated: {}".format(outputmappings))
        dbg2("[coordinator] setup_outputmappings finish: {}".format(outputmappings))
        return outputmappings

    def update_outputmappings(source_pe: GenericPE, required_pe: GenericPE, indices: List[int], outputmappings):
        for node_rank, mappings in setup_outputmappings(source_pe, required_pe, indices).items():
            if node_rank not in outputmappings:
                outputmappings[node_rank] = {}
            for output, mapping in mappings.items():
                if output not in outputmappings[node_rank]:
                    outputmappings[node_rank][output] = set(mapping)
                else:
                    outputmappings[node_rank][output].update(mapping)

    def assign_node(workflow: WorkflowGraph, pe: GenericPE, task_list: TaskList, is_source=False) -> List[int]:
        dbg2("[assign_node]")
        target_ranks = task_list.find_assignable(pe.numprocesses, is_source=is_source)
        dbg3("[assign_node] target_ranks:{}".format(target_ranks))
        for target_rank in target_ranks:
            dbg1("[assign_node] assigning:{}".format(target_rank))
            #comm.send(pe, target_rank, tag=TAG_DEPLOY)
            comm.send(pe.id, target_rank, tag=TAG_DEPLOY) #Is pe.id reliable for different processes (original mpi version assumes this)?
            dbg1("[assign_node] assigning:{} pe sent:{}".format(target_rank, pe.id))
            comm.send(target_ranks, target_rank, tag=TAG_BROTHER)
            dbg1("[assign_node] assigning:{} brothers sent:{}".format(target_rank,target_ranks))
            task_list.assign(target_rank, pe)
            dbg3("[assign_node] assigning:{} assignment recorded".format(target_rank))
        dbg2("[assign_node] finishing")
        return target_ranks

    #def onRequire(output_name: str, source_rank: int, workflow: WorkflowGraph, task_list: TaskList, outputmappings: IOMapping):
    def onRequire(output_name: str, source_rank: int, workflow: WorkflowGraph, task_list: TaskList):
        dbg1("[coordinator] onRequire")
        source_pe = task_list.get_node(source_rank)
        dbg2("[coordinator] source_pe: {}".format(source_pe))
        source_wfNode = workflow.objToNode[source_pe]
        dbg4("[coordinator] source_wfNode: {}".format(source_wfNode))
        target_wfNodes = []
        for (linked_wfNode, attributes) in workflow.graph[source_wfNode].items():
            if attributes['DIRECTION'][0] == source_pe:
                for fromConnection, _ in attributes['ALL_CONNECTIONS']:
                    if fromConnection == output_name:
                        target_wfNodes.append(linked_wfNode)
                        dbg4("[coordinator] target_wfNode found: {}".format(linked_wfNode))
        if not target_wfNodes:
            dbg1("[coordinator] encountered finial node of stream: {} [{}]".format(output_name, source_rank))
            comm.send([], source_rank, tag=TAG_TARGET)
            dbg1("[coordinator] targets:{} sent to {}".format([], source_rank))
            return
        dbg3("[coordinator] looping wfNodes: {}".format(target_wfNodes))
        all_indices = [] # Nested list - each element is another list (containing int)
        for required_pe in (wfNode.getContainedObject() for wfNode in target_wfNodes):
            dbg3("[coordinator] required_pe: {}".format(required_pe))
            dbg4("[coordinator] looking up if exists")
            indices = task_list.lookup(required_pe)
            dbg2("[coordinator] lookup finished: {}".format(indices))
            if len(indices) == 0:
                dbg2("[coordinator] no existing")
                indices = assign_node(workflow, required_pe, task_list)
                dbg3("[coordinator] new nodes assigned: {}".format(indices))
                # Won't work on circles
                #dbg2("[coordinator] updating outputmappings: {}".format(outputmappings))
                #update_outputmappings(source_pe, required_pe, indices, outputmappings)
                #dbg2("[coordinator] outputmappings updated: {}".format(outputmappings))
            all_indices.append(indices)
            #dbg2("[coordinator] updating outputmappings: {}".format(outputmappings))
            #update_outputmappings(source_pe, required_pe, indices, outputmappings)
            #dbg2("[coordinator] outputmappings updated: {}".format(outputmappings))
        dbg1("[coordinator] sending targets to {}".format(source_rank))
        comm.send(indices, source_rank, tag=TAG_TARGET)
        dbg1("[coordinator] targets sent: {}".format(indices))

        #dbg1("[coordinator] sending outputmapping to {}".format(source_rank))
        #comm.send(outputmappings[source_rank][output_name], source_rank, tag=TAG_OUTPUT_MAPPING)
        #dbg1("[coordinator] outputmapping sent: {}".format(outputmappings[source_rank]))

    dbg0("[coordinator]")
    dbg3("[coordinator] initialising")
    initial_nodes, numProcesses = getWorkflowProperty(workflow)
    task_list = TaskList(size, len(initial_nodes), numProcesses)
    #outputmappings = {}
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
            #onRequire(msg, source_rank, workflow, task_list, outputmappings)
            onRequire(msg, source_rank, workflow, task_list)
        elif tag == STATUS_TERMINATED:
            dbg0("[coordinator] onFinish")
            task_list.remove(source_rank)
            #outputmappings.pop(source_rank, None) #Nodes with no output connected won't be in the map
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
    #FIXME: workflow not used; remove it if really no need
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

    def __init__(self, workflow: WorkflowGraph, pe, brothers=[], provided_inputs=None):
        GenericWrapper.__init__(self, pe)
        self.workflow = workflow
        self.pe.log = types.MethodType(simpleLogger, pe)
        self.pe.rank = rank
        self.brothers = brothers
        self.provided_inputs = provided_inputs
        self.terminated = 0
        self._num_sources = len(pe.inputconnections)
        self.targets = {}
        self.fd = open("outputs/mpi_inc/{}".format(pe.id), 'a')

    def request_target(self, target: str, get_communication=True):
        dbg1("[{}] request_target: {}".format(rank, target))
        comm.send(target, RANK_COORDINATOR, tag=TAG_REQUIRE)
        dbg1("[{}] request sent".format(rank))
        dbg1("[{}] waiting for replies".format(rank))
        target_ranks = comm.recv(source=RANK_COORDINATOR, tag=TAG_TARGET)
        dbg1("[{}] [reply] target_ranks: {}".format(rank, target_ranks))
        if not target_ranks:
            return []
        if get_communication:
            output_mapping = comm.recv(source=RANK_COORDINATOR, tag=TAG_OUTPUT_MAPPING)
            dbg1("[{}] [reply] output_mapping: {}".format(rank, output_mapping))
            self.inform_output(target, output_mapping)
        dbg2("[{}] request_target finished".format(rank))
        return target_ranks

    def inform_output(self, output_name: str, output_mapping: List[Tuple[str,'Communication']]):
        dbg3("[{}] self.targets updating: [{}: {}]".format(rank, output_name, output_mapping))
        self.targets[output_name] = output_mapping
        dbg3("[{}] self.targets updated: {}".format(rank, self.targets))

    def create_communication_for_output(self, output_name: str):
        dbg1("[{}] creating communication for output: {}".format(rank, output_name))
        target_ranks = self.request_target(output_name, get_communication=False)
        node = self.workflow.objToNode[self.pe]
        graph = self.workflow.graph
        dbg2("[{}] creating communications")
        for edge in graph.edges(node, data=True):
            if edge[2]['DIRECTION'][0] == self.pe:
                allconnections = edge[2]['ALL_CONNECTIONS']
                for (source_output, dest_input) in allconnections:
                    if source_output == output_name:
                        try:
                            groupingtype = edge[2]['DIRECTION'][1].inputconnections[dest_input][GROUPING]
                        except KeyError:
                            groupingtype = None
                        communication = processor._getCommunication(self.brothers.index(rank), dest_input, target_ranks, groupingtype=groupingtype)
                        if output_name not in self.targets:
                            self.targets[output_name] = []
                        self.targets[output_name].append((dest_input, communication))
        dbg1("[{}] created communication for output: {} {}".format(rank, output_name, communication))

    def _read(self):
        dbg1("[{}] _read".format(rank))
        result = super(MPIIncWrapper, self)._read()
        if result is not None:
            dbg1("[{}] _read returning (with provided_inputs)".format(rank))
            return result

        status = MPI.Status()
        msg = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
        tag = status.Get_tag()
        while tag == STATUS_TERMINATED:
            self.terminated += 1
            if self.terminated >= self._num_sources:
                break
            else:
                msg = comm.recv(source=MPI.ANY_SOURCE,
                                tag=MPI.ANY_TAG,
                                status=status)
                tag = status.Get_tag()
        dbg1("[{}] _read returning: {} (tag:{})".format(rank, msg, tag))
        return msg, tag

    def _write(self, name, data):
        dbg1("[{}] _write name:{} data:{}".format(rank, name, data))
        #self.fd.write('[{}] {}\n'.format(name, data))
        if name not in self.targets:
            dbg1("[{}] target not existing".format(rank))
            #self.request_target(name)
            self.create_communication_for_output(name)
        try:
            targets = self.targets[name]
            dbg3("[{}] targets got: {}".format(rank, targets))
        except KeyError:
            # no targets
            self.pe.log('Produced output: %s' % {name: data})
            self.fd.write('[{}] {}\n'.format(name, data))
            dbg1("[{}] _write returning (encountered KeyError)".format(rank))
            return
        for (inputName, communication) in targets:
        #for communication in targets:
            dbg3("[{}] communication:{}".format(rank, communication))
            output = {inputName: data}
            dest = communication.getDestination(output)
            dbg4("[{}] name:{} data:{} dest:{}".format(rank, name, data, dest))
            for i in dest:
                try:
                    # self.pe.log('Sending %s to %s' % (output, i))
                    dbg1("[{}] sending {} to {}".format(rank, output, i))
                    request = comm.isend(output, tag=STATUS_ACTIVE, dest=i)
                    status = MPI.Status()
                    request.Wait(status)
                    dbg1("[{}] data sent".format(rank))
                except:
                    self.pe.log(
                        'Failed to send data stream "%s" to rank %s: %s'
                        % (name, i, traceback.format_exc()))
        dbg1("[{}] _write returning".format(rank))

    def _terminate(self):
        dbg1("[{}] _terminate".format(rank))
        for output, targets in self.targets.items():
            #for (inputName, communication) in targets:
            for communication in targets:
                for i in communication.destinations:
                    # self.pe.log('Terminating consumer %s' % i)
                    comm.isend(None, tag=STATUS_TERMINATED, dest=i)
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

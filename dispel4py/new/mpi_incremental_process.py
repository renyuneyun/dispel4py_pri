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

from threading import Thread, Lock, Event
from concurrent.futures import ThreadPoolExecutor
import time
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
TAG_QUERY = 54
TAG_WAIT = 53
TAG_TARGET = 52
TAG_BROTHER = 62
TAG_INPUT_MAPPING = 60
TAG_OUTPUT_MAPPING = 61
TAG_DATA = 0
TAG_FINALIZE = 100

RANK_COORDINATOR=0

def coordinator(workflow: WorkflowGraph, inputs, args):
    task_counter = 0
    pe_locks = {node.getContainedObject(): Lock() for node in workflow.graph.nodes()}

    direction_comm = comm.Dup()
    data_comm = comm.Dup()
    brother_comm = comm.Dup()

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
            raise Exception("shouldn't run out of nodes")
        def assign(self, index: int, pe: GenericPE):
            self.task_list[index] = pe
            self.max_used_nodes = max(self.max_used_nodes, len(self.working_nodes()))
        def working_nodes(self) -> List[int]: # May be replace with "num_working_nodes" because nowhere uses the actual nodes
            working = []
            for i, pe in enumerate(self.task_list):
                if i == 0: continue
                if pe != None:
                    working.append(i)
            return working
        def has_working_nodes(self) -> bool:
            for pe in self.task_list[1:]:
                if pe != None:
                    return True
            return False
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

    def assign_node(workflow: WorkflowGraph, pe: GenericPE, task_list: TaskList, is_source=True) -> List[int]:
        target_ranks = task_list.find_assignable(pe.numprocesses, is_source=is_source) #Needs list lock
        for target_rank in target_ranks:
            direction_comm.send(pe.id, target_rank, tag=TAG_DEPLOY) #Is pe.id reliable for different processes (original mpi version assumes this)?
            direction_comm.send(target_ranks, target_rank, tag=TAG_BROTHER)
            task_list.assign(target_rank, pe) #Needs list lock
        return target_ranks

    def onRequire(output_name: str, source_rank: int, workflow: WorkflowGraph, task_list: TaskList, deploy=True):
        nonlocal task_counter
        source_pe = task_list.get_node(source_rank) # Exists and won't disappear, so don't need lock
        all_indices = {}
        for (required_pe, allconnections) in workflow.outputConnections(source_pe):
            for (fromConnection, input_name) in allconnections:
                if fromConnection == output_name:
                    with pe_locks[required_pe]:
                        indices = task_list.lookup(required_pe) # needs global PE lock to know if another source is requiring the same pe
                        if len(indices) == 0 and deploy:
                            indices = assign_node(workflow, required_pe, task_list, is_source=False) #Holds global PE lock
                    if indices:
                        all_indices[(input_name, required_pe.id)] = indices
        task_counter -= 1
        direction_comm.send((output_name, all_indices), source_rank, tag=TAG_TARGET)
        #Decrease the counter

    initial_nodes, numProcesses = getWorkflowProperty(workflow)
    task_list = TaskList(size, len(initial_nodes), numProcesses)
    for node in initial_nodes:
        assign_node(workflow, node, task_list, is_source=True) # Parallel and don't need locks (because each other won't interfere and later nodes exist only after creation)

    status = MPI.Status()
    while True:
        msg = direction_comm.recv(status=status)
        source_rank, tag = status.Get_source(), status.Get_tag()
        if tag == TAG_REQUIRE: #Add one to a counter; node `source_rank` won't send TERMINATED when onRequire doesn't send targets back
            task_counter += 1
            Thread(target=onRequire, args=(msg, source_rank, workflow, task_list)).start()
            #onRequire(msg, source_rank, workflow, task_list)
        elif tag == TAG_QUERY:
            Thread(target=onRequire, args=(msg, source_rank, workflow, task_list, False)).start()
        elif tag == STATUS_TERMINATED: #Happens only when node `source_rank` has all destinations (i.e. no onRequire will be called for this node)
            task_list.remove(source_rank)
            if task_counter == 0 and not task_list.working_nodes(): #Because we know MPI guarentees FIFO for each pair's communication, we can safely say there is no request on-the-fly #We need a counter to know whether there are communications being processing by onRequire (which will cause later nodes' creation [and also later nodes' TERMINATED's sending] but currently no nodes are working
                for i in range(1, size):
                    direction_comm.isend(None, i, tag=TAG_FINALIZE)
                break
        else:
            raise Exception("unexpected tag")

    direction_comm.Free()
    data_comm.Free()
    brother_comm.Free()


def executor(workflow, inputs, args):
    status = MPI.Status()
    id_to_pe = {pe.id: pe for pe in (wfNode.getContainedObject() for wfNode in workflow.graph.nodes())}

    direction_comm = comm.Dup()
    data_comm = comm.Dup()
    brother_comm = comm.Dup()

    while True:
        msg = direction_comm.recv(source=RANK_COORDINATOR, status=status)
        tag = status.Get_tag()
        if tag == TAG_DEPLOY:
            pe_id = msg
            pe = id_to_pe[pe_id]
            brothers = direction_comm.recv(source=RANK_COORDINATOR, tag=TAG_BROTHER) #nodes executing the same pe (same node in the workflow graph)
            provided_inputs = processor.get_inputs(pe, inputs)
            wrapper = MPIIncWrapper(workflow, pe, brothers=brothers, provided_inputs=provided_inputs, direction_comm=direction_comm, data_comm=data_comm, brother_comm=brother_comm)
            wrapper.process()
        elif tag == TAG_FINALIZE:
            break
        else:
            raise Exception("Unknown/Illegal tag")

    direction_comm.Free()
    data_comm.Free()
    brother_comm.Free()

def process(workflow, inputs, args):
    t1 = time.time()
    if rank == 0:
        print([(edge[0].getContainedObject().id,edge[1].getContainedObject().id) for edge in workflow.graph.edges()])
    if rank == 0:
        coordinator(workflow, inputs, args)
    else:
        executor(workflow, inputs, args)
    t2 = time.time()
    if rank == 0:
        with open('measure/mpi_inc', 'a') as fd:
            fd.write("{}\n".format(t2-t1))


class MultithreadedWrapper(GenericWrapper):

    def __init__(self, pe):
        self.num_iterations = 0
        super(MultithreadedWrapper, self).__init__(pe)

    def _new_input(self, inputs):
        self.num_iterations += 1
        outputs = self.pe.process(inputs)
        if outputs is not None:
            # self.pe.log('Produced output: %s' % outputs)
            for key, value in outputs.items():
                self._write(key, value)

    def _listen(self):
        pass

    def process(self):
        self.pe.preprocess()
        self._listen()
        self.pe.postprocess()
        self._terminate()
        if self.num_iterations == 1:
            self.pe.log('Processed 1 iteration.')
        else:
            self.pe.log('Processed %s iterations.' % self.num_iterations)


class MPIIncWrapper(MultithreadedWrapper):

    def __init__(self, workflow: WorkflowGraph, pe: GenericPE, brothers=[], provided_inputs=None, direction_comm=comm, data_comm=comm, brother_comm=comm):
        super(MPIIncWrapper, self).__init__(pe)
        self.workflow = workflow
        self.pe.log = types.MethodType(simpleLogger, pe)
        self.pe.rank = rank
        self.brothers = brothers
        self.rep = brothers[0]
        self.provided_inputs = provided_inputs
        self.direction_comm = direction_comm
        self.data_comm = data_comm
        self.brother_comm = brother_comm
        self.terminated = 0
        self._num_sources = len(list(self.workflow.inputEdges(pe)))
        self.status = STATUS_ACTIVE
        self.request_locks = {output_name: Lock() for output_name in pe.outputconnections}
        self.request_events = {output_name: Event() for output_name in pe.outputconnections}
        self.executor = ThreadPoolExecutor(max_workers=3)
        self.targets = {}
        self.pending_messages = {}
        self.fd = open("outputs/mpi_inc/{}".format(pe.id), 'a')

    def is_rep(self):
        return self.rep == rank

    def _listen_direction(self):
        status = MPI.Status()
        while self.status == STATUS_ACTIVE:
            if not self.direction_comm.iprobe(source=RANK_COORDINATOR, status=status):
                time.sleep(0.001)
                continue
            msg = self.direction_comm.recv(source=RANK_COORDINATOR, status=status)
            tag = status.Get_tag()
            if tag == TAG_TARGET:
                output_name, all_indicies = msg
                self.create_communication_for_output(output_name, all_indicies)

    def _listen_data(self):
        inputs, status = self._read()
        while status != STATUS_TERMINATED:
            if inputs is not None:
                self.executor.submit(self._new_input, (inputs))
                #self._new_input(inputs)
            inputs, status = self._read()
        self.executor.shutdown()
        self.status = STATUS_TERMINATED

    def _listen(self):
        thread1 = Thread(target=self._listen_data)
        thread2 = Thread(target=self._listen_direction)
        thread1.start()
        thread2.start()
        thread1.join()
        thread2.join()

    def get_communication(self, output_name: str, deploy=True):
        with self.request_locks[output_name]:
            try:
                return self.targets[output_name]
            except KeyError:
                self.direction_comm.send(output_name, RANK_COORDINATOR, tag=TAG_REQUIRE if deploy else TAG_QUERY)
                self.request_events[output_name].wait()
                return self.targets[output_name]

    def create_communication_for_output(self, output_name: str, target_ranks_list: Dict[Tuple, List]):
        if target_ranks_list:
            for target_pe, allconnections in self.workflow.outputConnections(self.pe):
                for (source_output, dest_input) in allconnections:
                    if source_output == output_name:
                        target_ranks = target_ranks_list[(dest_input, target_pe.id)]
                        try:
                            groupingtype = target_pe.inputconnections[dest_input][GROUPING]
                        except KeyError:
                            groupingtype = None
                        communication = processor._getCommunication(self.brothers.index(rank), dest_input, target_ranks, groupingtype=groupingtype)
                        if output_name not in self.targets:
                            self.targets[output_name] = []
                        self.targets[output_name].append((dest_input, communication))
        else:
            self.targets[output_name] = []
        self.request_events[output_name].set()

    def _read(self):
        result = super(MPIIncWrapper, self)._read()
        if result is not None:
            return result

        status = MPI.Status()
        msg = self.data_comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
        source = status.Get_source()
        tag = status.Get_tag()
        #source, tag = status.Get_source(), status.Get_tag()
        while tag == STATUS_TERMINATED:
            self.terminated += 1
            if self.terminated >= self._num_sources:
                break
            msg = self.data_comm.recv(source=MPI.ANY_SOURCE,
                            tag=MPI.ANY_TAG,
                            status=status)
            source = status.Get_source()
            tag = status.Get_tag()
            #source, tag = status.Get_source(), status.Get_tag()
        return msg, tag

    def _write(self, name, data):
        if not self.pe.inputconnections:
            self.fd.write('[{}] {}\n'.format(name, data))
        targets = self.get_communication(name)
        if not targets:  # coordinator replied no targets
            self.pe.log('Produced output: %s' % {name: data})
            self.fd.write('[{}] {}\n'.format(name, data))
            return
        for (inputName, communication) in targets:
            output = {inputName: data}
            dest = communication.getDestination(output)
            for i in dest:
                try:
                    # self.pe.log('Sending %s to %s' % (output, i))
                    request = self.data_comm.issend(output, tag=STATUS_ACTIVE, dest=i)
                    req_key = (name, inputName, i)
                    self.pending_messages[req_key] = request
                except:
                    self.pe.log(
                        'Failed to send data stream "%s" to rank %s: %s'
                        % (name, i, traceback.format_exc()))

    def _terminate(self):
        if self.is_rep():
            status = MPI.Status()
            while self.brothers[1:]:
                msg = self.brother_comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
                source, tag = status.Get_source(), status.Get_tag()
                assert tag == STATUS_TERMINATED
                assert source in self.brothers
                assert not msg
                self.brothers.remove(source)

            for output in self.pe.outputconnections:
                #if output not in self.targets:
                #    self.create_communication_for_output(output)
                targets = self.get_communication(output, False)
                for (inputName, communication) in targets:
                    for i in communication.destinations:
                        # self.pe.log('Terminating consumer %s' % i)
                        self.data_comm.isend(None, tag=STATUS_TERMINATED, dest=i)
        else:
            MPI.Request.Waitall(list(self.pending_messages.values()))
            self.brother_comm.send(None, self.rep, tag=STATUS_TERMINATED)
        self.direction_comm.send(None, RANK_COORDINATOR, tag=STATUS_TERMINATED)


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

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

from threading import Thread, Lock, RLock, Event
from concurrent.futures import ThreadPoolExecutor
import time
import contextlib
from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = -1
size = 0

from dispel4py.new.processor\
    import GenericWrapper, simpleLogger, STATUS_TERMINATED, STATUS_ACTIVE
from dispel4py.new import processor

import argparse
import sys
import types
import traceback

DEBUG=2
def debug_fn(level):
    def fn(msg):
        if DEBUG >= level:
            print(msg)
    return fn
dbg0 = debug_fn(0) #node executing state (excuted or not; what it is)
dbg1 = debug_fn(1) #communications between nodes and vital function calling/returning
dbg2 = debug_fn(2) #main functions called/returing and main data
dbg3 = debug_fn(3) #secondary data
dbg4 = debug_fn(4) #detailed data


from dispel4py.core import GROUPING

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
    parser.add_argument('--spawned', help='spawed by framework (not for use manually)',
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
TAG_FINALIZE = 100
TAG_SPAWN_NEW_NODES = 97
TAG_SWITCH_CHANNEL = 95

RANK_COORDINATOR=0

def getWorkflowProperty(workflow):
    def initial_nodes(workflow):
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


class Coordinator(object):
    def __init__(self, workflow, inputs, args, size):
        dbg0("[coordinator]")
        dbg3("[coordinator] initialising")
        self.workflow = workflow
        self.initial_nodes, numProcesses = getWorkflowProperty(workflow)
        self.task_list = Coordinator.TaskList(size, len(self.initial_nodes), numProcesses)
        self.task_counter = 0
        self.pe_locks = {node.getContainedObject(): Lock() for node in workflow.graph.nodes()}
        self.node_comm = {}
        self.comm_lock = Lock()
        self.direction_comm = [comm.Dup()]
        self.data_comm = comm.Dup()
        self.brother_comm = comm.Dup()

    @property
    def size(self):
        return self.task_list.size + 1

    def run(self):
        dbg2("[coordinator] going to deploy initial nodes")
        for node in self.initial_nodes:
            self.assign_node(node, is_source=True) # Parallel and don't need locks (because each other won't interfere and later nodes exist only after creation)
        dbg2("[coordinator] initial nodes deployed")

        status = MPI.Status()
        finalized = False
        dbg0("[coordinator] waiting for request")
        while not finalized:
            for i in range(len(self.direction_comm)):
                comm = self.direction_comm[i]
                if comm.iprobe():
                    msg = comm.recv(status=status)
            #msg = direction_comm.recv(status=status)
                    source_rank, tag = status.Get_source(), status.Get_tag()
                    dbg1("[coordinator] request got: {} [from:{}] with tag {}".format(msg, source_rank, tag))
                    if tag == TAG_REQUIRE: #Add one to a counter; node `source_rank` won't send TERMINATED when onRequire doesn't send targets back
                        self.task_counter += 1
                        Thread(target=self.onRequire, args=(msg, source_rank)).start()
                        #onRequire(msg, source_rank, workflow, task_list)
                    elif tag == STATUS_TERMINATED: #Happens only when node `source_rank` has all destinations (i.e. no onRequire will be called for this node)
                        dbg0("[coordinator] onFinish")
                        self.task_list.remove(source_rank)
                        #del self.node_comm[source_rank]  # Do not need to delete (because it will be automatically overidden
                        comm.send(None, dest=source_rank, tag=TAG_SWITCH_CHANNEL)
                        if self.task_counter == 0 and not self.task_list.working_nodes(): #Because we know MPI guarentees FIFO for each pair's communication, we can safely say there is no request on-the-fly #We need a counter to know whether there are communications being processing by onRequire (which will cause later nodes' creation [and also later nodes' TERMINATED's sending] but currently no nodes are working
                            dbg1("[coordinator] sending finalize communication to all nodes")
                            for i in range(1, self.size):
                                self.direction_comm[-1].isend(None, i, tag=TAG_FINALIZE)
                            dbg1("[coordinator] finalize communication sent to all {} nodes".format(self.size))
                            finalized = True
                            break
                    else:
                        dbg0("[coordinator] unexpected tag: {} [from:{}]".format(tag, source_rank))
                        raise Exception("unexpected tag")
        dbg0("coordinator exit [max_used_nodes: {} (coordinator excluded)]".format(self.task_list.max_used_nodes))

    def assign_node(self, pe, is_source=True):
        dbg2("[assign_node]")
        found_enough_nodes = False
        with self.comm_lock:
            while not found_enough_nodes:
                # This section is sequential
                try:
                    target_ranks = self.task_list.find_assignable(pe.numprocesses, is_source=is_source, repeatable=pe.repeatable) #Needs list lock
                    found_enough_nodes = True
                except NoEnoughNodesException:
                    dbg1("[coordinator] NoEnoughNodesException")
                    num_to_spawn = 3
                    working_nodes = set(self.task_list.working_nodes())
                    for i in range(1, self.size):
                        if i in working_nodes:
                            self.node_comm[i].send(None, i, tag=TAG_SPAWN_NEW_NODES)
                        else:
                            self.direction_comm[-1].send(None, i, tag=TAG_SPAWN_NEW_NODES)
                    dbg1("[coordinator] spawning new nodes {}/{}".format(rank, self.size))
                    inter_comm = self.direction_comm[-1].Spawn(sys.argv[0], args=sys.argv[1:] + ['--spawned'], maxprocs=num_to_spawn, root=RANK_COORDINATOR)
                    #inter_comm = self.direction_comm[-1].Spawn("xterm", args=["-e"] + sys.argv + ['--spawned'], maxprocs=num_to_spawn, root=RANK_COORDINATOR)
                    dbg1('[coordinator] new nodes spawned')
                    dbg1('[coordinator] inter_comm remote_size {} self_rank {}/{}'.format(inter_comm.Get_remote_size(), inter_comm.Get_rank(), inter_comm.Get_size()))
                    new_direction_comm = inter_comm.Merge(high=False)
                    dbg1('[coordinator] inter_comm merged {}/{}'.format(new_direction_comm.Get_rank(), new_direction_comm.Get_size()))
                    direction_comm = new_direction_comm
                    dbg1('[coordinator] duping to data_comm')
                    data_comm = direction_comm.Dup()
                    dbg1('[coordinator] duping to brother_comm')
                    brother_comm = direction_comm.Dup()
                    dbg1('[coordinator] extending')
                    self.task_list.extend(num_to_spawn)
                    dbg1('[coordinator] extended')
                    self.direction_comm.append(direction_comm)
                    self.data_comm = data_comm
                    self.brother_comm = brother_comm

                    size = direction_comm.Get_size()
                    dbg1("[coordinator] new nodes spawned: expected {} now total {}".format(num_to_spawn, size))
            dbg3("[assign_node] target_ranks:{}".format(target_ranks))
            for target_rank in target_ranks:
                self.node_comm[target_rank] = self.direction_comm[-1]
                dbg1("[assign_node] assigning:{}".format(target_rank))
                self.node_comm[target_rank].send(pe.id, target_rank, tag=TAG_DEPLOY) #Is pe.id reliable for different processes (original mpi version assumes this)?
                dbg1("[assign_node] assigning:{} pe sent:{}".format(target_rank, pe.id))
                self.node_comm[target_rank].send(target_ranks, target_rank, tag=TAG_BROTHER)
                dbg1("[assign_node] assigning:{} brothers sent:{}".format(target_rank, target_ranks))
                self.task_list.assign(target_rank, pe) #Needs list lock
                dbg3("[assign_node] assigning:{} assignment recorded".format(target_rank))
            dbg2("[assign_node] finishing")
            return target_ranks

    def onRequire(self, output_name, source_rank):
        dbg1("[coordinator] onRequire")
        source_pe = self.task_list.get_node(source_rank) # Exists and won't disappear, so don't need lock
        dbg2("[coordinator] source_pe: {}".format(source_pe))
        all_indices = {}
        if not source_pe.repeatable:
            dbg2("[coordinator] source pe is not repeatable: {}@{}".format(source_pe, source_rank))
            for (required_pe, allconnections) in self.workflow.outputConnections(source_pe):
                for (fromConnection, input_name) in allconnections:
                    if fromConnection == output_name:
                        dbg3("[coordinator] required_pe: {}".format(required_pe))
                        with self.pe_locks[required_pe]:
                            dbg4("[coordinator] looking up if exists")
                            indices = self.task_list.lookup(required_pe) # needs global PE lock to know if another source is requiring the same pe
                            dbg2("[coordinator] lookup finished: {}".format(indices))
                            if len(indices) == 0:
                                dbg2("[coordinator] no existing")
                                indices = self.assign_node(required_pe, is_source=False) #Holds global PE lock
                                dbg3("[coordinator] new nodes assigned: {}".format(indices))
                        all_indices[(input_name, required_pe.id)] = indices
        else:
            dbg2("[coordinator] source pe is repeatable: {}@{}".format(source_pe, source_rank))
            circuit_inputs = source_pe.get_circuit(output_name)
            dbg3("[coordinator] circuit_inputs {}".format(circuit_inputs))
            if circuit_inputs:
                required_pe = source_pe
                with self.pe_locks[required_pe]:
                    dbg3("[coordinator] assigning new repeating nodes")
                    indices = self.assign_node(required_pe, is_source=False)
                    dbg3("[coordinator] new repeating nodes assigned: {}".format(indices))
                for input_name in circuit_inputs:
                    all_indices[(input_name, required_pe.id)] = indices
        self.task_counter -= 1
        dbg1("[coordinator] sending targets to {}".format(source_rank))
        self.node_comm[source_rank].send((output_name, all_indices), source_rank, tag=TAG_TARGET)
        dbg1("[coordinator] targets sent: {}".format(all_indices))

    class TaskList:
        def __init__(self, size, numSources=-1, totalProcesses=-1):
            self.task_list = [None] * size
            self.task_list[0] = 0 # special mark for coordinator
            self.max_used_nodes = 0
            self.numSources = numSources
            self.totalProcesses = totalProcesses
        @property
        def size(self):
            return len(self.task_list) - 1
        def find_assignable(self, numproc=1, is_source=False, repeatable=False):
            assignables = []
            if is_source or repeatable:
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
            raise NoEnoughNodesException("shouldn't run out of nodes")
        def assign(self, index, pe):
            self.task_list[index] = pe
            self.max_used_nodes = max(self.max_used_nodes, len(self.working_nodes()))
        def working_nodes(self): # May be replace with "num_working_nodes" because nowhere uses the actual nodes
            working = []
            for i, pe in enumerate(self.task_list):
                if i == 0: continue
                if pe != None:
                    working.append(i)
            return working
        def has_working_nodes(self):
            for pe in self.task_list[1:]:
                if pe != None:
                    return True
            return False
        def lookup(self, target_pe):
            matches = []
            for i, pe in enumerate(self.task_list):
                if i == 0: continue
                if pe == target_pe:
                    matches.append(i)
            return matches
        def get_node(self, index):
            return self.task_list[index]
        def remove(self, index):
            self.task_list[index] = None
            dbg2("TaskList after removing: {}".format(self.task_list))
        def extend(self, size):
            self.task_list += [None] * size


def coordinator(workflow, inputs, args):
    coord = Coordinator(workflow, inputs, args, size)
    coord.run()


class Executor(object):
    '''
    Single-threaded
    Reads three kinds of directions from coordinator: SPAWN, DEPLOY, FINALIZE
        To handle corner-cases, TAG_SWITCH_CHANNEL is also needed
    '''
    def __init__(self, workflow, inputs, args):
        global rank, size, comm
        dbg0("[executor {}] is {}spawned".format(rank, '' if args.spawned else 'not '))
        self.workflow = workflow
        self.inputs = inputs
        if not args.spawned:
            direction_comm = comm.Dup()
            data_comm = comm.Dup()
            brother_comm = comm.Dup()
        else:
            inter_comm = MPI.Comm.Get_parent()
            dbg1('[{}] (spawned node) inter_comm {}'.format(rank, inter_comm))
            dbg1('[{}] (spawned node) inter_comm remote_size {} self_rank {}/{}'.format(rank, inter_comm.Get_remote_size(), inter_comm.Get_rank(), inter_comm.Get_size()))
            new_direction_comm = inter_comm.Merge(high=True)
            dbg1('[{}] (spawned node) inter_comm merged'.format(rank))
            rank = new_direction_comm.Get_rank()
            size = new_direction_comm.Get_size()
            direction_comm = new_direction_comm
            dbg1('[{}] (spawned node) duping to data_comm'.format(rank))
            data_comm = direction_comm.Dup()
            dbg1('[{}] (spawned node) duping to brother_comm'.format(rank))
            brother_comm = direction_comm.Dup()
            dbg1('[{}] (spawned node) duping finished'.format(rank))
        self.rank = rank
        self.size = size
        self.old_direction_comm = None  # Will be used in case Wapper exits while Coordinator sends TAG_SPAWN_NEW_NODES
        self.direction_comm = direction_comm
        self.data_comm = data_comm
        self.brother_comm = brother_comm

    def do_spawn(self):
        dbg2("[executor {}] spawning new nodes {}/{}".format(rank, self.rank, self.size))
        inter_comm = self.direction_comm.Spawn('', root=RANK_COORDINATOR)
        dbg2("[executor {}] new nodes spawned".format(rank))
        dbg1('[executor {}] inter_comm remote_size {} self_rank {}/{}'.format(rank, inter_comm.Get_remote_size(), inter_comm.Get_rank(), inter_comm.Get_size()))
        new_direction_comm = inter_comm.Merge(high=False)
        dbg2("[executor {}] inter_comm merged".format(rank))
        direction_comm = new_direction_comm
        dbg2("[executor {}] duping to data_comm".format(rank))
        data_comm = direction_comm.Dup()
        dbg2("[executor {}] duping to brother_comm".format(rank))
        brother_comm = direction_comm.Dup()
        dbg2("[executor {}] duping finished".format(rank))
        self.direction_comm = direction_comm
        self.data_comm = data_comm
        self.brother_comm = brother_comm
        self.size = new_direction_comm.Get_size()

    def run(self):
        id_to_pe = {pe.id: pe for pe in (wfNode.getContainedObject() for wfNode in self.workflow.graph.nodes())}
        status = MPI.Status()
        finalized = False
        read_old = False
        dbg0("[executor {}] waiting for communication from coordinator".format(rank))
        while not finalized:
#            time.sleep(0.001)
#            if self.direction_comm.iprobe(source=RANK_COORDINATOR):
            if read_old:
                msg = self.old_direction_comm.recv(source=RANK_COORDINATOR, status=status)
                tag = status.Get_tag()
                dbg1("[executor {}] old communication got: tag:{}".format(rank, tag))
                if tag == TAG_SWITCH_CHANNEL:
                    read_old = False
                elif tag == TAG_SPAWN_NEW_NODES:
                    self.do_spawn()
                else:
                    raise RuntimeError("Unexpected tag through old communicator")
            else:
                msg = self.direction_comm.recv(source=RANK_COORDINATOR, status=status)
        #msg = direction_comm.recv(source=RANK_COORDINATOR, status=status)
                tag = status.Get_tag()
                dbg1("[executor {}] communication got: tag:{}".format(rank, tag))
                if tag == TAG_DEPLOY:
                    pe_id = msg
                    pe = id_to_pe[pe_id]
                    dbg1("[executor {}] node received: {}".format(rank, pe.id))
                    brothers = self.direction_comm.recv(source=RANK_COORDINATOR, tag=TAG_BROTHER) #nodes executing the same pe (same node in the workflow graph)
                    dbg1("[executor {}] brothers received: {}".format(rank, brothers))
                    dbg4("[executor {}] going to get_inputs".format(rank))
                    provided_inputs = processor.get_inputs(pe, self.inputs)
                    dbg4("[executor {}] finished get_inputs: {}".format(rank, provided_inputs))
                    wrapper = MPIIncWrapper(self.workflow, pe, brothers=brothers, provided_inputs=provided_inputs, direction_comm=self.direction_comm, data_comm=self.data_comm, brother_comm=self.brother_comm)
                    dbg0("[executor {}] finished creating wrapper - executing".format(rank))
                    wrapper.process()
                    read_old = True
                    self.old_direction_comm = self.direction_comm
                    self.direction_comm = wrapper._direction_comm[-1]
                    self.data_comm = wrapper._data_comm[-1]
                    self.brother_comm = wrapper._brother_comm[-1]
                elif tag == TAG_FINALIZE:
                    finalized = True
                    break
                elif tag == TAG_SPAWN_NEW_NODES:
                    self.do_spawn()
        dbg0("[executor {}] finishing".format(rank))


def executor(workflow, inputs, args):
    myexecutor = Executor(workflow, inputs, args)
    myexecutor.run()

def process(workflow, inputs, args):
    global rank, size
    rank = comm.Get_rank()
    size = comm.Get_size()
    if rank == 0:
        print([(edge[0].getContainedObject().id,edge[1].getContainedObject().id) for edge in workflow.graph.edges()])
    if not args.spawned and rank == 0:
        t1 = time.time()
        coordinator(workflow, inputs, args)
        t2 = time.time()
        dbg0("logging")
        with open('measure/mpi_inc', 'a') as fd:
            dbg0("logging opened")
            fd.write("{}\n".format(t2-t1))
            dbg0("logging written")
        dbg0("logging finished")
    else:
        executor(workflow, inputs, args)


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

    def __init__(self, workflow, pe, brothers, provided_inputs, direction_comm, data_comm, brother_comm):
        super(MPIIncWrapper, self).__init__(pe)
        self.workflow = workflow
        self.pe.log = types.MethodType(simpleLogger, pe)
        self.pe.rank = rank
        self.rank = rank
        self.size = direction_comm.Get_size()
        self.brothers = brothers
        self.rep = brothers[0]
        self.provided_inputs = provided_inputs
        self.comm_lock = RLock()
        self._direction_comm = [direction_comm]  # Don't need to switch (but need ordered) because the initial one would satisfy the use
        self._direction_lock = RLock()
        self._data_comm_for_target = {}
        self._data_comm = [data_comm]  # Need ordered and (choose to) use a dedicated one to send and loop all to read
        self._data_lock = RLock()
        self._brother_comm = [brother_comm]  # Same as direction_comm, don't need to switch (and need ordered)
        self._brother_lock = RLock()
        self.terminated = 0
        self._num_sources = len(list(self.workflow.inputEdges(pe)))
        self.status = STATUS_ACTIVE
        self.request_locks = {output_name: Lock() for output_name in pe.outputconnections}
        self.request_events = {output_name: Event() for output_name in pe.outputconnections}
        self.executor = ThreadPoolExecutor(max_workers=3)
        self.targets = {}
        self.pending_messages = []
        self.fd = open("outputs/mpi_inc/{}".format(pe.id), 'a')

    def _get_comm(self, comm, sep_lock, index):
        self.comm_lock.acquire()
        with sep_lock:
            self.comm_lock.release()
            yield comm[index]

    @property
    @contextlib.contextmanager
    def direction_comm(self):
        with self.get_direction_comm() as ret:
            yield ret

    @contextlib.contextmanager
    def get_direction_comm(self, index=0):
        for ret in self._get_comm(self._direction_comm, self._direction_lock, index):
            yield ret  # This way and the commented way are the same, but this can supress lint warning
        #return self._get_comm(self._direction_comm, self._direction_lock, index)

    @property
    @contextlib.contextmanager
    def data_comm(self):
        with self.get_data_comm() as ret:
            yield ret

    @contextlib.contextmanager
    def get_data_comm(self, index=0):
        for ret in self._get_comm(self._data_comm, self._data_lock, index):
            yield ret
        #return self._get_comm(self._data_comm, self._data_lock, index)

    @property
    @contextlib.contextmanager
    def brother_comm(self):
        with self.get_brother_comm() as ret:
            yield ret

    @contextlib.contextmanager
    def get_brother_comm(self, index=0):
        for ret in self._get_comm(self._brother_comm, self._brother_lock, index):
            yield ret
        #return self._get_comm(self._brother_comm, self._brother_lock, index)

    def is_rep(self):
        return self.rep == self.rank

    def _listen_direction(self):
        status = MPI.Status()
        while self.status == STATUS_ACTIVE:
            with self.direction_comm as direction_comm:
                dbg4("[{}] direction_comm: {}".format(self.rank, direction_comm))
                if not direction_comm.iprobe(source=RANK_COORDINATOR, status=status):
                    continue
                msg = direction_comm.recv(source=RANK_COORDINATOR, status=status)
                tag = status.Get_tag()
                dbg1("[{}] direction got: {} with tag {}".format(rank, msg, tag))
                if tag == TAG_TARGET:
                    output_name, all_indicies = msg
                    self.create_communication_for_output(output_name, all_indicies)
                elif tag == TAG_SPAWN_NEW_NODES:
                    dbg2("[{}] spawn new nodes prepare".format(rank))
                    with self.comm_lock:
                        dbg2("[{}] spawn new nodes prepared comm_lock".format(rank))
                        with self.get_direction_comm(-1) as l_direction_comm:
                            dbg2("[{}] spawning new nodes {}/{}".format(self.rank, self.rank, self.size))
                            inter_comm = l_direction_comm.Spawn('', root=RANK_COORDINATOR)
                            dbg2("[{}] new nodes spawned".format(rank))
                            dbg2('[{}] inter_comm remote_size {} self_rank {}/{}'.format(rank, inter_comm.Get_remote_size(), inter_comm.Get_rank(), inter_comm.Get_size()))
                            new_direction_comm = inter_comm.Merge(high=False)
                            dbg2("[{}] inter_comm merged".format(rank))
                            dbg2("[{}] duping to data_comm".format(rank))
                            data_comm = new_direction_comm.Dup()
                            dbg2("[{}] duping to brother_comm".format(rank))
                            brother_comm = new_direction_comm.Dup()
                            dbg2("[{}] duping finished".format(rank))

                            self._direction_comm.append(new_direction_comm)
                            self._data_comm.append(data_comm)
                            self._brother_comm.append(brother_comm)
                            self.size = new_direction_comm.Get_size()

    def _listen_data(self):
        inputs, status = self._read()
        while status != STATUS_TERMINATED:
            if inputs is not None:
                if not self.pe.FIFO:
                    self.executor.submit(self._new_input, (inputs))
                else:
                    self._new_input(inputs)
                #self._new_input(inputs)
            inputs, status = self._read()
        if not self.pe.FIFO:
            self.executor.shutdown()
            dbg1("[{}] executor shutted down".format(rank))
        self.status = STATUS_TERMINATED

    def _listen(self):
        thread1 = Thread(target=self._listen_data)
        thread2 = Thread(target=self._listen_direction)
        thread1.start()
        thread2.start()
        thread1.join()
        thread2.join()

    def get_communication(self, output_name, existing=False):
        with self.request_locks[output_name]:
            try:
                return self.targets[output_name]
            except KeyError:
                if not existing:
                    dbg1("[{}] target not existing".format(rank))
                    dbg1("[{}] request_target: {}".format(rank, output_name))
                    with self.get_direction_comm(-1) as direction_comm:
                        direction_comm.send(output_name, RANK_COORDINATOR, tag=TAG_REQUIRE)
                    dbg1("[{}] request sent".format(rank))
                    self.request_events[output_name].wait()
                    dbg1("[{}] get_communication returning".format(rank))
                    return self.targets[output_name]
                else:
                    return []

    def create_communication_for_output(self, output_name, target_ranks_list):
        if target_ranks_list:
            for target_pe, allconnections in self.workflow.outputConnections(self.pe):
                for (source_output, dest_input) in allconnections:
                    if source_output == output_name:
                        target_ranks = target_ranks_list[(dest_input, target_pe.id)]
                        try:
                            groupingtype = target_pe.inputconnections[dest_input][GROUPING]
                        except KeyError:
                            groupingtype = None
                        communication = processor._getCommunication(self.brothers.index(rank), dest_input, target_ranks, groupingtype)
                        if output_name not in self.targets:
                            self.targets[output_name] = []
                        self.targets[output_name].append((dest_input, communication))
        else:
            self.targets[output_name] = []
        self.request_events[output_name].set()

    def _read(self):
        '''
        This method will never be called in parallel
        '''
        result = super(MPIIncWrapper, self)._read()
        if result is not None:
            return result

        status = MPI.Status()
        shall_break = False
        while not shall_break:
            for i in range(len(self._data_comm)):
                with self.get_data_comm(i) as data_comm:
                    if data_comm.iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG):
                        msg = data_comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
                        source, tag = status.Get_source(), status.Get_tag()
                        if tag == STATUS_TERMINATED:
                            self.terminated += 1
                            if self.terminated >= self._num_sources:
                                shall_break = True
                        else:
                            shall_break = True
                        if shall_break:
                            break
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
                    with self._data_lock:
                        try:
                            data_comm = self._data_comm_for_target[i]  # Needs to confirm to use `i` or `name`
                        except KeyError:
                            with self.get_data_comm(-1) as i_data_comm:
                                data_comm = i_data_comm
                                self._data_comm_for_target[i] = data_comm
                    request = data_comm.issend(output, tag=STATUS_ACTIVE, dest=i)
                    req_key = (name, inputName, i)
                    self.pending_messages.append(request)
                except:
                    self.pe.log(
                        'Failed to send data stream "%s" to rank %s: %s'
                        % (name, i, traceback.format_exc()))

    def _terminate(self):
        if self.is_rep():
            status = MPI.Status()
            while self.brothers[1:]:
                with self.brother_comm as brother_comm:
                    msg = brother_comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
                    source, tag = status.Get_source(), status.Get_tag()
                    assert tag == STATUS_TERMINATED
                    assert source in self.brothers
                    assert not msg
                    self.brothers.remove(source)

            for output in self.pe.outputconnections:
                #if output not in self.targets:
                #    self.create_communication_for_output(output)
                targets = self.get_communication(output, existing=True)
                with self.get_data_comm(-1) as data_comm:
                    for (inputName, communication) in targets:
                        for i in communication.destinations:
                            # self.pe.log('Terminating consumer %s' % i)
                            data_comm.isend(None, tag=STATUS_TERMINATED, dest=i)
        else:
            MPI.Request.Waitall(self.pending_messages)
            with self.brother_comm as brother_comm:
                brother_comm.send(None, self.rep, tag=STATUS_TERMINATED)
        with self.direction_comm as direction_comm:
            direction_comm.send(None, RANK_COORDINATOR, tag=STATUS_TERMINATED)


class NoEnoughNodesException(Exception):
    pass


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

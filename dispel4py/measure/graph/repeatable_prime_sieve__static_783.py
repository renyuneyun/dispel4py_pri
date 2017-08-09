
from dispel4py.workflow_graph import WorkflowGraph

from dispel4py.examples.graph_testing.testing_PEs    import IntegerProducer, RepeatablePrimeSieve

producer = IntegerProducer(2, 6000)
sieve = RepeatablePrimeSieve()
graph = WorkflowGraph()
graph.connect(producer, 'output', sieve, RepeatablePrimeSieve.INPUT_NUMBER_LINE)

prev = sieve
for i in range(1, 783):
    sieve = RepeatablePrimeSieve()
    graph.connect(prev, RepeatablePrimeSieve.OUTPUT_NUMBER_LINE, sieve, RepeatablePrimeSieve.INPUT_NUMBER_LINE)
    prev = sieve

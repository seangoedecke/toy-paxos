require_relative './node'
require_relative './cluster'
require_relative './network'

# Sets up a cluster with three nodes on an unreliable network and makes a write
# NB: Paxos only comes to consensus on one decision. You need something else tracking round numbers to get you to
# multiple decisions or a unified log

DROP_CHANCE = 0
SHUFFLE_CHANCE = 2
NUMBER_OF_NODES = 3

network = Network.new(DROP_CHANCE, SHUFFLE_CHANCE)
cluster = PaxosCluster.new
NUMBER_OF_NODES.times do
  PaxosNode.new(cluster, network)
end

cluster.write 'bar'
cluster.write 'foo' # Feel free to add more writes here. With a sufficiently unreliable network, any write can win
network.process_queue # Force any messages that are waiting in the queue to get sent

puts cluster.get_consensus.inspect

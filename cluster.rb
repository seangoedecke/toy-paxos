# A group of Paxos nodes. Clients can read or write to this cluster, which will select a random
# node to write or read to.
class PaxosCluster
  attr_reader :nodes

  def initialize()
    @nodes = []
  end

  def register(node)
    @nodes.push(node)
  end

  # Writes and reads are done to the cluster, not to individual nodes
  def write(message)
    @nodes.sample.write(message) # since any node might get any write request, pick a random node to send the message to
  end

  def read
    # read synchronously
    @nodes.sample.read # any node could get a read request. data isn't guaranteed to be up-to-date
  end

  def get_consensus # returns a hash of all the values => how many nodes have them
    values = @nodes.map do |n|
      n.read.last && n.read.last.value
    end

    res = Hash.new(0)
    values.each { |v| res[v] += 1 }
    res 
  end
end


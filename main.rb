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
end

class Network
  def initialize
    @queue = []
  end

  def send_message(target, message)
    sleep(rand)
    @queue.push([target, message])
    if rand(10) > 5
      @queue.shuffle!
      process_queue # only send messages occasionally
    end
  end

  def process_queue
    @queue.each do |e|
      e[0].receive(e[1])
    end
  end
end

class PaxosNode
  attr_accessor :highest_sequence_number

  def initialize(cluster, network)
    @network = network
    @highest_sequence_number = 0
    @last_agreed_value = nil
    @cluster = cluster
    cluster.register(self)
    @is_proposer = false # a node won't think it's a proposer until it gets a write request
    @log = [] # what the node thinks its current state is
    @value = nil # a value that the node is trying to gain consensus on, if it's the proposer
    @proposed_sequence_number = 0
    @promises_received = 0
  end

  def receive(message)
    puts "#{self.to_s} #{self.proposer? ? '(proposer)' : '(acceptor)' } received #{message[:type]} with seq #{message[:sequence_number]}"

    case message[:type]
    when "prepare"
      return if message[:sequence_number] <= highest_sequence_number
      @highest_sequence_number = message[:sequence_number]

      if proposer?
        # escalate proposal, since someone's trying to propose a higher sequence than yours
        puts "escalating!"
        write(@value)
      end

      @highest_sequence_number = message[:sequence_number]
      send_message(message[:sender], new_message('promise')) # TODO: send last received value?

    when "promise"
      return unless proposer?

      @promises_received += 1
 
      puts "leader got #{@promises_received}. quorum needed is #{quorum_size}"
      if @promises_received >= quorum_size
        other_nodes.each do |node|
          send_message(node, new_message('commit', value: @value, sequence_number: @proposed_sequence_number))
        end
        @log.push(@value)

        # give up proposer status
        @is_proposer = false # TODO: extract
        @promises_received = 0
      end

    when "commit"
      return if proposer?
      return if message[:sequence_number] < @highest_sequence_number

      @log.push(message[:value])

    else
      raise "Invalid message received: #{message}"
    end
  end

  def send_message(target, message)

    puts "#{self.to_s} #{self.proposer? ? '(proposer)' : '(acceptor)' } sent #{message[:type]} with seq #{message[:sequence_number]}"
    @network.send_message(target, message)
  end

  def read
    @log
  end

  def write(value)
    @is_proposer = true
    @value = value
    @proposed_sequence_number = highest_sequence_number + 1
    other_nodes.each do |node|
      send_message(node, new_message('prepare', sequence_number: @proposed_sequence_number))
    end
  end

  def proposer?
    @is_proposer
  end

  private

  def new_message(type, value: nil, sequence_number: nil)
    { type: type, sender: self, sequence_number: sequence_number, value: value }
  end

  def other_nodes
    @cluster.nodes.reject{ |n| n == self }
  end

  def quorum_size
    (@cluster.nodes.count / 2) + 1
  end

end


def build_cluster(network)
  cluster = PaxosCluster.new
  3.times do
    PaxosNode.new(cluster, network)
  end
  cluster
end

network = Network.new
c = build_cluster(network)
puts "Writing 1..."
c.write 1
puts "Writing 2..."
c.write 2
network.process_queue # flush queue
puts c.read.inspect


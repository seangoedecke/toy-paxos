class Connection
  def send_message(target, message)
    target.receive(message)
  end
end

class PaxosCluster
  attr_reader :nodes

  def initialize
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
    @nodes.sample.read # any node could get a read request. data isn't guaranteed to be up-to-date
  end
end

class Message
  attr_reader :sender, :payload
  def initialize(sender, payload)
    @sender = sender
    @payload = payload
  end
end

class PaxosNode
  attr_accessor :highest_sequence_number

  def initialize(cluster)
    @connection = Connection.new
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
      return if proposer?
      return if message[:sequence_number] <= highest_sequence_number

      @highest_sequence_number = message[:sequence_number]
      send_message(message[:sender], new_message('promise')) # TODO: send last receieved value?

    when "promise"
      return unless proposer?

      @promises_received += 1
 
      puts "leader got #{@promises_received}. quorum needed is #{quorum_size}"
      if @promises_received >= quorum_size
        other_nodes.each do |node|
          send_message(node, new_message('commit', value: @value, sequence_number: @proposed_sequence_number))
        end
        @log.push(message[:value])

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
    @connection.send_message(target, message)
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

def build_cluster
  cluster = PaxosCluster.new
  3.times do
    PaxosNode.new(cluster)
  end
  cluster
end

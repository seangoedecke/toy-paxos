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

  def get_consensus
    values = @nodes.map do |n|
      n.read.last && n.read.last.value
    end

    res = Hash.new(0)
    values.each { |v| res[v] += 1 }
    res 
  end
end

class Network
  def initialize
    @queue = []
  end

  def send_message(target, message)
    @queue.push([target, message]) if rand(10) > 2 # sometimes lose messages 
    if rand(10) > 4
      @queue.shuffle! # sometimes send messages out-of-order
      process_queue # only send messages occasionally
    end
  end

  def process_queue
    @queue.each do |e|
      e[0].receive(e[1])
    end
  end
end

class SequenceNumber
  include Comparable
  attr_reader :num, :sender

  def initialize(num, sender)
    @sender = sender
    @num = num
  end

  def to_s
    "#{num} - #{sender.object_id}"
  end

  def +(int)
    SequenceNumber.new(num + int, sender)
  end

  def <=>(other)
    if num == other.num
      sender.object_id <=> other.sender.object_id
    else
      num <=> other.num
    end
  end
end

class Message 
  attr_reader :type, :sequence_number, :value, :sender
  def initialize(type, sequence_number, value, sender:)
    @type = type
    if sequence_number.is_a? SequenceNumber
      @sequence_number = SequenceNumber.new(sequence_number.num, sender)
    else
      @sequence_number = SequenceNumber.new(sequence_number, sender)
    end
    @value = value
    @sender = sender
  end
  def to_s
    "#{@type} message: seq #{@sequence_number}, value: #{@value}"
  end
end

class PaxosNode

  def initialize(cluster, network)
    @network = network
    @promised_sequence_number = SequenceNumber.new(0, self)
    @last_agreed_value = nil
    @cluster = cluster
    cluster.register(self)
    @is_proposer = false # a node won't think it's a proposer until it gets a write request
    @log = [] # what the node thinks its current state is
    @value = nil # a value that the node is trying to gain consensus on, if it's the proposer
    @highest_sequence_number = SequenceNumber.new(0, self)
    @promises_received = 0
    @highest_sequence_number_from_acceptors = SequenceNumber.new(0, self)
  end

  def receive(message)
    puts "#{self.to_s} #{self.proposer? ? '(proposer)' : '(acceptor)' } received #{message.to_s}"

    case message.type
    when "prepare"
      return if message.sequence_number <= @highest_sequence_number

      if proposer?
        # escalate proposal, since someone's trying to propose a higher sequence than yours
        @highest_sequence_number = message.sequence_number
        puts "got proposal with #{message.sequence_number}. escalating to #{@highest_sequence_number + 1}!"
        write(@value)
        return
      end

      puts "updating highest seq number to #{message.sequence_number}"
      @highest_sequence_number = message.sequence_number
      send_message(message.sender, new_message('promise', value: @log.last, sequence_number: @highest_sequence_number)) 

    when "promise"
      return unless proposer?
      return if message.sequence_number <= @highest_sequence_number

      @promises_received += 1

      puts "leader got #{@promises_received}. quorum needed is #{quorum_size}"

      # if any other proposers have a value, need to use that
      if message.value && message.value.sequence_number > @highest_sequence_number_from_acceptors
        @highest_sequence_number_from_acceptors = message.value.sequence_number
        puts "Using value #{message.value.value}"
        @value = message.value.value
      end

      if @promises_received >= quorum_size
        other_nodes.each do |node|
          send_message(node, new_message('commit', value: @value, sequence_number: @highest_sequence_number))
        end

        @log.push(new_message('commit', value: @value, sequence_number: @highest_sequence_number))

        # give up proposer status
        @is_proposer = false # TODO: extract
        @promises_received = 0
        if @log.empty?
          @highest_sequence_number = SequenceNumber.new(0, self)
        else
          @highest_sequence_number = @log.last.sequence_number
        end
      end

    when "commit"
      return if proposer?

      if message.sequence_number < @highest_sequence_number 
        puts "Rejecting commit - it's older than #{@highest_sequence_number.to_s}"
        return
      end

      @highest_sequence_number = message.sequence_number
      @log.push(message)
      puts "Committed! #{@log.last.value}"
    end
  end

  def send_message(target, message)

    puts "#{self.to_s} #{self.proposer? ? '(proposer)' : '(acceptor)' } sent #{message.to_s}"
    @network.send_message(target, message)
  end

  def read
    @log
  end

  def write(value)
    @is_proposer = true
    @value = value
    @highest_sequence_number = @highest_sequence_number + 1
    @highest_sequence_number_from_acceptors = SequenceNumber.new(0, self)
    other_nodes.each do |node|
      send_message(node, new_message('prepare', sequence_number: @highest_sequence_number))
    end
  end

  def proposer?
    @is_proposer
  end

  private

  def new_message(type, value: nil, sequence_number: nil)
    Message.new(type, sequence_number, value, sender: self)
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

# NB: Paxos only comes to consensus on one decision. You need something else tracking round numbers to get you to
# multiple decisions or a unified log

network = Network.new
c = build_cluster(network)
c.write 'foo'


puts "Flushing queue"
network.process_queue # flush queue
puts c.get_consensus.inspect

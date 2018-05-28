require_relative './sequence_number'
require_relative './message'

# An implementation of a Paxos node. This is where all the magic happens and also where it's
# the most incomplete. A node can be a proposer or an acceptor, but not both simultaneously
class PaxosNode
  def initialize(cluster, network)
    @cluster = cluster
    cluster.register(self)
    @network = network

    @is_proposer = false # a node won't think it's a proposer until it gets a write request
    @log = [] # what the node thinks its current state is
    @highest_sequence_number = SequenceNumber.new(0, self) # the highest sequence number the node has seen

    # Only relevant for proposers 
    @value = nil # a value that the node is trying to gain consensus on, if it's the proposer
    @promises_received = 0 # Proposers track this to check if they've reached quorum
    @highest_sequence_number_from_acceptors = SequenceNumber.new(0, self)
  end

  def receive(message)
    puts "#{self.to_s} #{self.proposer? ? '(proposer)' : '(acceptor)' } received #{message.to_s}"

    case message.type

    # Acceptor nodes receive prepare messages when a proposer is trying to write a value
    # They respond with promises if the associated sequence number is higher than they've seen before
    when "prepare"
      return if message.sequence_number <= @highest_sequence_number

      if proposer?
        # If someone's trying to propose a higher sequence than yours, give up on your
        # current proposal and make a new one with a higher sequence number
        @highest_sequence_number = message.sequence_number
        puts "got proposal with #{message.sequence_number}. escalating to #{@highest_sequence_number + 1}!"
        write(@value)
        return
      end
      
      # Keep track of the message's sequence number, since you're promising to ignore proposals older than
      # it. In a non-toy implementation, we would write this to persistent storage in case the node crashes
      @highest_sequence_number = message.sequence_number
      send_message(message.sender, new_message('promise', value: @log.last, sequence_number: @highest_sequence_number)) 

    # Proposer nodes receive promise messages when acceptor nodes agree to listen to their proposal
    # They respond with commit messages that actually contain the value proposed
    when "promise"
      return unless proposer?
      return if message.sequence_number < @highest_sequence_number # Ignore old promises


      # if any other proposers have a value, need to use that
      if message.value && message.value.sequence_number > @highest_sequence_number_from_acceptors
        @highest_sequence_number_from_acceptors = message.value.sequence_number
        puts "Using value #{message.value.value}"
        @value = message.value.value
      end

      # The proposer tracks how many promises it received. If it's reached quorum, it sends everyone
      # commit messages and gives up being a proposer (NB: this means it stops listening to other promises)
      @promises_received += 1
      if @promises_received >= quorum_size
        # send everyone commit messages
        other_nodes.each do |node|
          send_message(node, new_message('commit', value: @value, sequence_number: @highest_sequence_number))
        end
        
        # commit the value to its own data store
        @log.push(new_message('commit', value: @value, sequence_number: @highest_sequence_number))

        # give up proposer status and reset associated instance variables
        @is_proposer = false # TODO: extract
        @promises_received = 0
        if @log.empty?
          @highest_sequence_number = SequenceNumber.new(0, self)
        else
          @highest_sequence_number = @log.last.sequence_number
        end
      end

    # Acceptor nodes receive commit messages when the proposer reaches quorum on a proposal
    # They write the value to their data store in response (assuming the sequence number is
    # not lower than the one they've agreed to).
    when "commit"
      return if proposer?
      return if message.sequence_number < @highest_sequence_number 

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
    # become the proposer and send prepare messages
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


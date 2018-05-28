require_relative './sequence_number'

# The messages that nodes send back and forth. Can be prepares, promises or commits.
# For this implementation, we don't send Nacks
class Message 
  attr_reader :type, :sequence_number, :value, :sender
  def initialize(type, sequence_number, value, sender:)
    @type = type
    if sequence_number.is_a? SequenceNumber # allow messages to be created with ints or existing sequence nums
      @sequence_number = SequenceNumber.new(sequence_number.num, sender)
    else
      @sequence_number = SequenceNumber.new(sequence_number, sender)
    end
    @value = value
    @sender = sender
  end
  def to_s
    "#{@type} message (#{@sequence_number})#{@value ? " with value: #{value}" : ''}"
  end
end

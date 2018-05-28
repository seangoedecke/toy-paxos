# Paxos nodes need sequence numbers that are orderable and unique. We achieve uniqueness by namespacing them per object
# id of the node that generated them. We achieve orderability by comparing first on the number and second on the object 
# id of the node.

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

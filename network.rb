# Simulates an unreliable network: basically a message queue where messages sometimes
# get dropped or sent out-of-order
class Network
  def initialize(drop_chance, shuffle_chance)
    @queue = []
    @drop_chance = drop_chance
    @shuffle_chance = shuffle_chance
  end

  def send_message(target, message)
    @queue.push([target, message]) if rand(10) > @drop_chance # sometimes lose messages 
    if rand(10) > @shuffle_chance 
      @queue.shuffle! # sometimes send messages out-of-order
      process_queue # only send messages occasionally, to make sure we have some in the queue to shuffle
    end
  end

  def process_queue
    @queue.each do |e|
      e[0].receive(e[1])
    end
  end
end

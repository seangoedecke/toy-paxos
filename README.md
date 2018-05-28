# Toy Paxos

A minimal implementation of Paxos in Ruby. Uses an unreliable messaging queue to simulate communication over the network. Guaranteed to achieve quorum on a single result. Not guaranteed to be accurate yet.

See `node.rb` for the good stuff, or clone and run `ruby main.rb` to simulate a Paxos round yourself. You can tweak the constants in that file for a more unreliable network.

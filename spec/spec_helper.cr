require "spec"
require "../src/nsq"
require "expect"

#def assert_no_timeout(time = 1, &block)
#  expect{
#    Timeout::timeout(time) do
#      yield
#    end
#  }.not_to raise_error
#end
#
#def assert_timeout(time = 1, &block)
#  expect{
#    Timeout::timeout(time) do
#      yield
#    end
#  }.to raise_error(Timeout::Error)
#end
#
## Block execution until a condition is met
## Times out after 30 seconds by default
##
## example:
##   wait_for { @consumer.queue.length > 0 }
##
#def wait_for(timeout = 30, &block)
#  Timeout::timeout(timeout) do
#    loop do
#      break if yield
#      sleep(0.1)
#    end
#  end
#end

TOPIC = "some-topic"
CHANNEL = "some-channel"

def new_consumer(opts = new Hash(Symbol, Array(String)|String|Integer))
  lookupd = @cluster.nsqlookupd.map{|l| "#{l.host}:#{l.http_port}"}
  Nsq::Consumer.new({
    topic: TOPIC,
    channel: CHANNEL,
    nsqlookupd: lookupd,
    max_in_flight: 1
  }.merge(opts))
end


def new_producer(nsqd, opts = new Hash(Symbol, Array(String)|String|Integer))
  Nsq::Producer.new({
    topic: TOPIC,
    nsqd: "#{nsqd.host}:#{nsqd.tcp_port}",
    discovery_interval: 1
  }.merge(opts))
end

def new_lookupd_producer(opts = new Hash(Symbol, Array(String)|String|Integer))
  lookupd = @cluster.nsqlookupd.map{|l| "#{l.host}:#{l.http_port}"}
  Nsq::Producer.new({
    topic: TOPIC,
    nsqlookupd: lookupd,
    discovery_interval: 1
  }.merge(opts))
end

# This is for certain spots where we're testing connections going up and down.
# Don't want these tests to take forever to run!
def set_speedy_connection_timeouts!
  allow_any_instance_of(Nsq::Connection).to receive(:snooze) { sleep 0.01 }
end
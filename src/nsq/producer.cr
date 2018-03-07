# require_relative "client_base"

module Nsq
  class Producer < ClientBase
    getter :topic

    def initialize(opts = new Hash(Symbol, Array(String) | String | Int32 | Channel(Message | Symbol)))
      @shutdown = false
      @discovery_interval = 30
      if opts.has_key?(:nsqlookupd) && opts[:nsqlookupd].is_a?(String)
        @nsqlookupds = [opts[:nsqlookupd].as(String)]
      elsif opts.has_key?(:nsqlookupd) && opts[:nsqlookupd].is_a?(Array(String))
        @nsqlookupds = opts[:nsqlookupd].as(Array(String))
      else
        @nsqlookupds = Array(String).new
      end

      if opts.has_key?(:topic)
        @topic = opts[:topic].as(String)
      else
        raise(ArgumentError.new("topic is required"))
      end

      if opts.has_key?(:msg_timeout)
        @msg_timeout = opts[:msg_timeout].as(Int32)
      else
        @msg_timeout = 60_000_i32 # 60s
      end

      # @ssl_context = opts[:ssl_context]
      # @tls_options = opts[:tls_options]
      # @tls_v1 = opts[:tls_v1]

      # This is where we keep a record of our active nsqd connections
      # The key is a string with the host and port of the instance (e.g.
      # '127.0.0.1:4150') and the key is the Connection instance.
      @connections = Hash(String, Connection).new

      if !@nsqlookupds.empty?
        discover_repeatedly({
          :nsqlookupds => @nsqlookupds,
          :topic       => @topic,
          :interval    => @discovery_interval,
        })
      else
        # normally, we find nsqd instances to connect to via nsqlookupd(s)
        # in this case let's connect to an nsqd instance directly
        nsqd = "127.0.0.1:4150"
        if opts.has_key?(:nsqd)
          nsqd = opts[:nsqd].as(String)
        end
        add_connection(nsqd, Opts.new)
      end
    end

    def write(*raw_messages : Array(String) | String)
      if !@topic
        raise "No topic specified. Either specify a topic when instantiating the Producer or use write_to_topic."
      end

      write_to_topic(@topic, *raw_messages)
    end

    # Arg 'delay' in seconds
    def deferred_write(delay, *raw_messages : Array(String) | String)
      if !@topic
        raise "No topic specified. Either specify a topic when instantiating the Producer or use write_to_topic."
      end
      if delay < 0.0
        raise "Delay can't be negative, use a positive float."
      end

      deferred_write_to_topic(@topic, delay, *raw_messages)
    end

    def write_to_topic(topic, *raw_messages : Array(String) | String)
      # return error if message(s) not provided
      raise ArgumentError.new("message not provided") if raw_messages.empty?

      # stringify the messages
      messages = raw_messages

      # get a suitable connection to write to
      connection = connection_for_write

      if messages.size > 1
        connection.mpub(topic, messages)
      else
        connection.pub(topic, messages.first)
      end
    end

    def deferred_write_to_topic(topic, delay, *raw_messages : Array(String) | String)
      raise ArgumentError.new("message not provided") if raw_messages.empty?
      messages = raw_messages # .map(&:to_s)
      connection = connection_for_write
      messages.each do |msg|
        connection.dpub(topic, (delay * 1000).to_i, msg)
      end
    end

    private def connection_for_write
      # Choose a random Connection that's currently connected
      # Or, if there's nothing connected, just take any random one
      connections_currently_connected = connections.select { |_, c| c.connected? }
      connection = connections_currently_connected.values.sample || connections.values.sample

      # Raise an exception if there's no connection available
      unless connection
        raise "No connections available"
      end

      connection
    end
  end
end

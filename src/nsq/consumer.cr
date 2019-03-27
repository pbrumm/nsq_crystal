# require_relative "client_base"

module Nsq
  class Consumer < ClientBase
    getter :max_in_flight

    def initialize(opts = new Hash(Symbol, Array(String) | String | Int32 | Channel(Message | Symbol)))
      @shutdown = false
      @discovery_interval = 30
      @max_in_flight = 1
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

      if opts.has_key?(:channel)
        @channel = opts[:channel].as(String)
      else
        raise(ArgumentError.new("channel is required"))
      end

      if opts.has_key?(:msg_timeout)
        @msg_timeout = opts[:msg_timeout].as(Int32)
      else
        @msg_timeout = 60_000_i32 # 60s
      end
      if opts.has_key?(:max_in_flight)
        @max_in_flight = opts[:max_in_flight].as(Int32)
      end
      # @ssl_context = opts[:ssl_context]
      # @tls_options = opts[:tls_options]
      # @tls_v1 = opts[:tls_v1]

      # This is where we queue up the messages we receive from each connection
      if opts.has_key?(:queue) && opts[:queue].is_a?(Channel(Message | Symbol))
        @messages = opts[:queue].as(Channel(Message | Symbol))
      else
        @messages = Channel(Message | Symbol).new
      end

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
        nsqd = nil
        if opts.has_key?(:nsqd)
          nsqd = opts[:nsqd].as(String)
        end
        add_connection(nsqd || "127.0.0.1:4150", {:max_in_flight => @max_in_flight})
      end
    end

    # pop the next message off the queue
    def pop : Message | Nil
      # p [:waiting_for_pop]
      m = @messages.receive
      # p m
      if m.is_a?(Symbol)
        nil
      else
        m.as(Message)
      end
    end

    # By default, if the internal queue is empty, pop will block until
    # a new message comes in.
    #
    # Calling this method won't block. If there are no messages, it just
    # returns nil.
    def pop_without_blocking : Message
      @messages.wait_for_receive
    rescue ThreadError
      # When the Queue is empty calling `Queue#pop(true)` will raise a ThreadError
      nil
    end

    # returns the number of messages we have locally in the queue
    def size
      @messages.size
    end

    private def add_connection(nsqd, options = Opts.new)
      options ||= Opts.new
      connection = super(nsqd.as(String), {
        :topic         => @topic,
        :channel       => @channel,
        :queue         => @messages,
        :msg_timeout   => @msg_timeout,
        :max_in_flight => @max_in_flight,
      }.merge(options))
      # connection.topic_create(@topic)
      connection
    end

    # Be conservative, but don't set a connection's max_in_flight below 1
    private def max_in_flight_per_connection(number_of_connections = @connections.size)
      [@max_in_flight / number_of_connections, 1].max
    end

    private def connections_changed
      spawn do
        redistribute_ready
      end
    end

    private def redistribute_ready
      @connections.values.each do |connection|
        connection.max_in_flight = max_in_flight_per_connection
        connection.re_up_ready
      end
    end
  end
end

# require_relative "discovery"
# require_relative "connection"
# require_relative "logger"

module Nsq
  class ClientBase
    include Nsq::AttributeLogger
    @@log_attributes = [:topic]

    getter :topic
    getter :connections
    @discovery_active = true

    def connected?
      @connections.values.any? { |c| !c.connected? }
    end

    def terminate
      @discovery_active = false
      drop_all_connections
    end

    def topic_delete(topic = nil, channel = nil)
      @connections.values.each { |connection|
        connection.topic_delete(topic)
      }
      return true
    end

    def channel_depth(topic = nil, channel = nil)
      count = 0
      @connections.values.each { |connection|
        c = connection.channel_depth(topic, channel)
        if c
          count += c
        end
      }
      return count
    end

    def topic_msgcount(topic = nil)
      count = 0
      @connections.values.each { |connection|
        c = connection.topic_msgcount(topic)
        if c
          count += c
        end
      }
      return count
    end

    def channel_empty(topic = nil, channel = nil)
      @connections.values.each { |connection|
        connection.channel_empty(topic, channel)
      }
      return true
    end

    # discovers nsqds from an nsqlookupd repeatedly
    #
    #   opts:
    #     nsqlookups: ['127.0.0.1:4161'],
    #     topic: 'topic-to-find-nsqds-for',
    #     interval: 60
    #

    private def discover_repeatedly(opts : Opts)
      spawn do
        @discovery = Discovery.new(opts[:nsqlookupds].as(Array(String)))

        loop do
          begin
            break unless @discovery_active
            nsqds = nsqds_from_lookupd(opts[:topic].as(String))
            if nsqds
              drop_and_add_connections(nsqds.as(Array(String)))
            end
          rescue DiscoveryException
            # We can't connect to any nsqlookupds. That's okay, we'll just
            # leave our current nsqd connections alone and try again later.
            # warn "Could not connect to any nsqlookupd instances in discovery loop"
          end
          sleep opts[:interval].as(Int)
        end
      end
    end

    private def nsqds_from_lookupd(topic : String | Nil)
      if @discovery
        d = @discovery.as(Discovery)
        if topic
          d.nsqds_for_topic(topic)
        else
          d.nsqds
        end
      end
    end

    private def drop_and_add_connections(nsqds : Array(String))
      # drop nsqd connections that are no longer in lookupd
      missing_nsqds = @connections.keys - nsqds
      missing_nsqds.each do |nsqd|
        drop_connection(nsqd)
      end

      # add new ones
      new_nsqds = nsqds - @connections.keys
      new_nsqds.each do |nsqd|
        begin
          add_connection(nsqd, Opts.new)
        rescue ex : Exception
          error "Failed to connect to nsqd @ #{nsqd}: #{ex}"
        end
      end

      # balance RDY state amongst the connections
      connections_changed
    end

    private def add_connection(nsqd : String, options : Opts)
      info "+ Adding connection #{nsqd}"
      host, port = nsqd.split(':')
      connection = Connection.new({
        :host => host,
        :port => port.to_i,
        # :ssl_context => @ssl_context,
        # :tls_options => @tls_options,
        # :tls_v1 => @tls_v1,
      }.merge(options))
      @connections[nsqd] = connection
    end

    private def drop_connection(nsqd)
      info "- Dropping connection #{nsqd}"
      connection = @connections.delete(nsqd)
      connection.close if connection
      connections_changed
    end

    private def drop_all_connections
      @connections.keys.each do |nsqd|
        drop_connection(nsqd)
      end
    end

    # optional subclass hook
    private def connections_changed
    end
  end
end

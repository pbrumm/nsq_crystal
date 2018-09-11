require "json"
require "http/client"
require "uri"

# require_relative "logger"

# Connects to nsqlookup's to find the nsqd instances for a given topic
module Nsq
  class Discovery
    include Nsq::AttributeLogger
    @lookupds : Array(String)

    # lookupd addresses must be formatted like so: '<host>:<http-port>'
    def initialize(lookupds = Array(String))
      @lookupds = lookupds
    end

    # Returns an array of nsqds instances
    #
    # nsqd instances returned are strings in this format: '<host>:<tcp-port>'
    #
    #     discovery.nsqds
    #     #=> ['127.0.0.1:4150', '127.0.0.1:4152']
    #
    # If all nsqlookupd's are unreachable, raises Nsq::DiscoveryException
    #
    def nsqds
      gather_nsqds_from_all_lookupds do |lookupd|
        get_nsqds(lookupd)
      end
    end

    # Returns an array of nsqds instances that have messages for
    # that topic.
    #
    # nsqd instances returned are strings in this format: '<host>:<tcp-port>'
    #
    #     discovery.nsqds_for_topic('a-topic')
    #     #=> ['127.0.0.1:4150', '127.0.0.1:4152']
    #
    # If all nsqlookupd's are unreachable, raises Nsq::DiscoveryException
    #
    def nsqds_for_topic(topic : String)
      # p [:in_for_topic, topic]
      gather_nsqds_from_all_lookupds do |lookupd|
        get_nsqds(lookupd, topic)
      end
    end

    private def gather_nsqds_from_all_lookupds
      nsqd_list = @lookupds.map do |lookupd|
        yield(lookupd)
      end.flatten

      # All nsqlookupds were unreachable, raise an error!
      if nsqd_list.size > 0 && nsqd_list.all? { |nsqd| nsqd.nil? }
        raise DiscoveryException.new
      end

      nsqd_list.compact.uniq
    end

    # Returns an array of nsqd addresses
    # If there's an error, return nil
    private def get_nsqds(lookupd, topic : String | Nil = nil)
      uri_scheme = "http://" unless lookupd.match(%r(https?://))
      uri = URI.parse("#{uri_scheme}#{lookupd}")

      query = "ts=#{Time.now.epoch}"
      if topic
        uri.path = "/lookup"
        query += "&topic=#{URI.escape(topic.as(String))}"
      else
        uri.path = "/nodes"
      end
      uri.query = query

      begin
        response = HTTP::Client.get(uri)

        if response.status_code == 200
          data = JSON.parse(response.body)
          producers = data["producers"].as_a if data["producers"].as_a? # v1.0.0-compat
          producers ||= data["data"]["producers"].as_a if data["data"]? && data["data"]["producers"].as_a?
          if producers
            return producers.map do |producer|
              "#{producer["broadcast_address"]}:#{producer["tcp_port"]}"
            end
          else
            return Array(String).new
          end
        else
          return Array(String).new
        end
      rescue e : Exception
        error "Error during discovery for #{lookupd}: #{e}"
        nil
      end
    end
  end
end

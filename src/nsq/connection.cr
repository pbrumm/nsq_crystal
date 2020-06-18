require "json"
require "socket"
require "openssl"
require "http/client"

# require "timeout"

# require_relative "frames/error"
# require_relative "frames/message"
# require_relative "frames/response"
# require_relative "logger"

module Nsq
  class Connection
    include Nsq::AttributeLogger
    @@log_attributes = [:host, :port]

    getter :host
    getter :port
    property :max_in_flight
    getter :presumed_in_flight

    USER_AGENT         = "nsq-crystal/#{Nsq::VERSION}"
    RESPONSE_HEARTBEAT = "_heartbeat_"
    RESPONSE_OK        = "OK"
    CLOSE_WAIT         = "CLOSE_WAIT"
    ERROR_INVALID      = "E_INVALID"

    @queue : Channel(Message | Symbol)
    @server_version : String | Nil
    @max_in_flight : Int32
    @last_heartbeat : Time | Nil
    @connect_timed_out = false
    @connect_completed = false
    presumed_in_flight = 0
    @connected_through_lookupd = false
    @in_reconnecting_loop = false

    def initialize(opts : Opts)
      @host = opts[:host].as(String) || (raise ArgumentError.new("host is required"))
      @port = opts[:port].as(Int32) || (raise ArgumentError.new("port is required"))

      if opts.has_key?(:queue) && opts[:queue].is_a?(Channel(Message | Symbol))
        @queue = opts[:queue].as(Channel(Message | Symbol))
      else
        @queue = Channel(Message | Symbol).new
      end
      if opts.has_key?(:topic)
        @topic = opts[:topic].as(String)
      else
        @topic = nil
      end

      if opts.has_key?(:channel)
        @channel = opts[:channel].as(String)
      else
        @channel = nil
      end

      if opts.has_key?(:msg_timeout)
        @msg_timeout = opts[:msg_timeout].as(Int32)
      else
        @msg_timeout = 60_000_i32 # 60s
      end
      if opts.has_key?(:max_in_flight)
        @max_in_flight = opts[:max_in_flight].as(Int32)
      else
        @max_in_flight = 1
      end

      if opts.has_key?(:connected_through_lookupd) && opts[:connected_through_lookupd].is_a?(Bool)
        @connected_through_lookupd = opts[:connected_through_lookupd].as(Bool)
      end
      # if opts.has_key?(:tls_options)
      #   @tls_options = opts[:tls_options].as(Int64)
      # else
      #   @tls_options = nil
      # end
      # @tls_options = nil#opts[:tls_options]
      # if opts[:ssl_context]
      #   if @tls_options
      #     warn "ssl_context and tls_options both set. Using tls_options. Ignoring ssl_context."
      #   else
      #     @tls_options = opts[:ssl_context]
      #     warn "ssl_context will be deprecated nsq-ruby version 3. Please use tls_options instead."
      #   end
      # end
      # @tls_v1 = !!opts[:tls_v1]

      # if @tls_options
      #   if @tls_v1
      #     validate_tls_options!
      #   else
      #     warn "tls_options was provided, but tls_v1 is false. Skipping validation of tls_options."
      #   end
      # end

      if @msg_timeout < 1000
        raise ArgumentError.new("msg_timeout cannot be less than 1000. it's in milliseconds.")
      end

      # for outgoing communication
      @write_queue = Channel(String | Symbol).new

      # For indicating that the connection has died.
      # We use a Queue so we don't have to poll. Used to communicate across
      # threads (from write_loop and read_loop to connect_and_monitor).
      @death_queue = Channel(String | Exception | Symbol).new

      @connected = false
      @presumed_in_flight = 0

      on_successful_connection = Proc(Nil, Nil).new {
        start_monitoring_connection
      }
      open_connection(on_successful_connection)
    end

    def stats(topic = nil, channel = nil)
      additional = ""
      if topic
        additional = "&topic=#{topic}"
      end
      if channel
        additional = "&channel=#{channel}"
      end
      stats_timed_out = false
      stats_completed = false
      response : HTTP::Client::Response | Nil = nil
      stats_wait_until = 30.seconds.from_now
      spawn do
        client = HTTP::Client.new(@host, @port + 1)
        client.connect_timeout = 10.seconds
        client.read_timeout = 10.seconds
        response = client.get("/stats?format=json#{additional}")
        stats_completed = true
      end

      loop do
        if stats_completed
          break
        else
          if stats_wait_until > Time.utc
            sleep 0.05
          else
            raise "stats_timed_out"
          end
        end
      end
      if response && response.as(HTTP::Client::Response).status_code == 200
        data = JSON.parse(response.as(HTTP::Client::Response).body)
        return data
      else
        return nil
      end
    end

    def channel_depth(topic : String, channel : String)
      data = stats(topic, channel)
      if data
        d : JSON::Any
        if data["data"]?
          d = data["data"]
        else
          d = data
        end
        if d["topics"]?
          topics_info = d["topics"]
          topic_matches = topics_info.select { |t| t["topic_name"] == topic }
          if topic_matches.size > 0
            if topic_info
              # p topic_info
              return topic_info["depth"].as_i
            end
          end
        end
      end
      return 0
    end

    def topic_msgcount(topic : String)
      data = stats(topic)
      if data
        d : Nil | JSON::Any
        if data["data"]?
          d = data["data"]
        else
          d = data
        end
        if d["topics"]? && d["topics"].as_a?
          topics_info = d["topics"]
          topic_matches = topics_info.as_a.select { |t| t["topic_name"] == topic }
          if topic_matches.size > 0
            topic_info = topic_matches.first
            if topic_info
              # p topic_info
              return topic_info["message_count"].as_i
            end
          end
        end
      end
      return 0
    end

    def topic_delete(topic = nil)
      additional = ""
      if topic
        additional = "topic=#{topic}"
      end

      response = HTTP::Client.post("http://#{@host}:#{@port + 1}/topic/delete?#{additional}")
      if response.status_code == 200
        return true
      else
        return false
      end
    end

    def channel_empty(topic = nil, channel = nil)
      additional = ""
      if topic
        additional = "&topic=#{topic}"
      end
      if channel
        additional = "&channel=#{channel}"
      end
      response = HTTP::Client.post("http://#{@host}:#{@port + 1}/stats?format=json#{additional}")
      if response.status_code == 200
        return true
      else
        return false
      end
    end

    def connected?
      if @connected
        if @last_heartbeat.is_a?(Time)
          if @last_heartbeat.as(Time) > 40.seconds.ago
            true
          else
            false
          end
        else
          true
        end
      else
        false
      end
    end

    # close the connection and don't try to re-open it
    def close
      unless @in_reconnecting_loop
        stop_monitoring_connection
        close_connection
      end
    end

    def sub(topic, channel)
      write "SUB #{topic} #{channel}\n"
    end

    def rdy(count)
      write "RDY #{count}\n"
    end

    def fin(message_id : String)
      write "FIN #{message_id}\n"
      decrement_in_flight
    end

    def req(message_id, timeout)
      write "REQ #{message_id} #{timeout}\n"
      decrement_in_flight
    end

    def touch(message_id)
      write "TOUCH #{message_id}\n"
    end

    def pub(topic, message : String)
      command = "PUB #{topic}\n"
      io = IO::Memory.new # (command.size + 1 + message.bytesize)
      io.write(command.to_slice)
      io.write_bytes(message.bytesize, IO::ByteFormat::BigEndian)
      io.write(message.to_slice)
      write(io.to_s)
      # write  message.bytesize, message].pack("a*l>a*")
    end

    def dpub(topic, delay_in_ms, message)
      # write ["DPUB #{topic} #{delay_in_ms}\n", message.bytesize, message].pack("a*l>a*")
      command = "DPUB #{topic} #{delay_in_ms}\n"
      io = IO::Memory.new # (command.size + 1 + message.bytesize)
      io.write(command.to_slice)
      io.write_bytes(message.bytesize, IO::ByteFormat::BigEndian)
      io.write(message.to_slice)
      write(io.to_s)
    end

    def mpub(topic, messages)
      body = messages.map do |message|
        # [message.bytesize, message].pack("l>a*")
        io = IO::Memory.new # (1 + message.bytesize)
        io.write_bytes(message.bytesize, IO::ByteFormat::BigEndian)
        io.write(message.to_slice)
        io.to_s
      end.join

      # write ["MPUB #{topic}\n", body.bytesize, messages.size, body].pack("a*l>l>a*")
      command = "MPUB #{topic}\n"
      io = IO::Memory.new # (command.size + 1 + 1 + body.bytesize)
      io.write(command.to_slice)
      io.write_bytes(body.bytesize, IO::ByteFormat::BigEndian)
      io.write_bytes(messages.size, IO::ByteFormat::BigEndian)
      io.write(body.to_slice)
      write(io.to_s)
    end

    # Tell the server we are ready for more messages!
    def re_up_ready
      rdy(@max_in_flight)
      # assume these messages are coming our way. yes, this might not be the
      # case, but it's much easier to manage our RDY state with the server if
      # we treat things this way.
      @presumed_in_flight = @max_in_flight
    end

    private def cls
      write "CLS\n"
    end

    private def nop
      write "NOP\n"
    end

    private def write(raw : String)
      @write_queue.send(raw)
    end

    private def socket_block
      # debug ">>> #{raw.inspect}"
      yield @socket.as(TCPSocket)
    end

    private def identify
      hostname = System.hostname + ":" + Process.pid.to_s
      # hostname = "test"
      metadata = {
        client_id:             hostname,
        hostname:              hostname,
        feature_negotiation:   true,
        heartbeat_interval:    30_000, # 30 seconds
        output_buffer:         16_000, # 16kb
        output_buffer_timeout: 250,    # 250ms
        # tls_v1: @tls_v1,
        snappy:      false,
        deflate:     false,
        sample_rate: 0, # disable sampling
        user_agent:  USER_AGENT,
        msg_timeout: @msg_timeout,
      }.to_json

      socket_block { |socket|
        socket.write("IDENTIFY\n".to_slice)
        socket.write_bytes(metadata.bytesize, IO::ByteFormat::NetworkEndian)
        socket.write(metadata.to_slice)
      } # [, metadata.bytesize, metadata].pack("a*l>a*")

      # Now wait for the response!
      frame = receive_frame.as(Frame)
      server = JSON.parse(frame.data)
      if @max_in_flight > server["max_rdy_count"].as_i
        raise "max_in_flight is set to #{@max_in_flight}, server only supports #{server["max_rdy_count"]}"
      end

      @server_version = server["version"].as_s
    end

    private def handle_response(frame)
      case frame.data
      when RESPONSE_HEARTBEAT
        debug "Received heartbeat"
        @last_heartbeat = Time.utc
        nop
      when RESPONSE_OK
        debug "Received OK"
      when CLOSE_WAIT
        return false
      else
        die "Received response we don't know how to handle: #{frame.data}"
        return false
      end
      return true
    end

    private def receive_frame
      size = IO::ByteFormat::NetworkEndian.decode(Int32, @socket.as(IO))
      type = IO::ByteFormat::NetworkEndian.decode(Int32, @socket.as(IO))
      raise IO::TimeoutError.new if @connect_timed_out

      if type
        size -= 4 # we want the size of the data part and type already took up 4 bytes
        data = Slice(UInt8).new(size)
        # p [:read_data, size]
        @socket.as(IO).read(data)
        raise IO::TimeoutError.new if @connect_timed_out
        frame_class = frame_class_for_type(type)
        # p [:read_data_done]
        return frame_class.new(data, self)
      end
    end

    FRAME_CLASSES = [Response, Error, Message]

    private def frame_class_for_type(type)
      raise "Bad frame type specified: #{type}" if type > FRAME_CLASSES.size - 1
      [Response, Error, Message][type]
    end

    private def decrement_in_flight
      @presumed_in_flight -= 1

      if server_needs_rdy_re_ups?
        # now that we're less than @max_in_flight we might need to re-up our RDY state
        threshold = (@max_in_flight * 0.2).ceil
        re_up_ready if @presumed_in_flight <= threshold
      end
    end

    private def start_read_loop
      @read_loop_active = true
      spawn { read_loop }
    end

    private def stop_read_loop
      @queue.send(:stop_read_loop)
      @read_loop_active = false
    end

    private def read_loop
      loop do
        frame = receive_frame
        # p [:frame, frame]
        if frame.is_a?(Response)
          if !handle_response(frame)
            break
          end
        elsif frame.is_a?(Error)
          error "Error received: #{frame.data}"
        elsif frame.is_a?(Message)
          debug "<<< #{frame.body}"
          @queue.send(frame) if @queue
        else
          raise "No data from socket"
        end
      end
    rescue ex : Exception
      die(ex)
    end

    private def start_write_loop
      @write_loop_active = true
      spawn { write_loop }
    end

    private def stop_write_loop
      if @write_loop_active
        @write_queue.send(:stop_write_loop)
      end
      @write_loop_active = false
    end

    private def write_loop
      data = nil
      loop do
        data = @write_queue.receive
        # p [:write, data]
        if data.is_a?(Symbol)
          if data == :stop_write_loop
            break
          end
        else
          socket_block { |socket| socket.write(data.as(String).to_slice) }
        end
      end
    rescue ex : Exception
      # requeue PUB and MPUB commands
      if data =~ /^M?PUB/
        debug "Requeueing to write_queue: #{data.inspect}"
        @write_queue.send(data.as(String))
      end
      die(ex)
    end

    # Waits for death of connection
    private def start_monitoring_connection
      # @connection_monitor_thread ||= Thread.new{monitor_connection}
      # @connection_monitor_thread.abort_on_exception = true
      @connection_monitor_active = true
      spawn { monitor_connection }
    end

    private def stop_monitoring_connection
      if @connection_monitor_active
        @connection_monitor_active = false
        @death_queue.send(:stop_connection_loop)
      else
        @connection_monitor_active = false
      end
    end

    private def monitor_connection
      loop do
        # wait for death, hopefully it never comes
        cause_of_death = @death_queue.receive
        break unless @connection_monitor_active
        # p [:death, cause_of_death]
        warn "Died from: #{cause_of_death}"

        debug "Reconnecting..."
        begin
          reconnect
        rescue e : Exception
          sleep 0.01
        end
        debug "Reconnected!"

        # clear all death messages, since we're now reconnected.
        # we don't want to complete this loop and immediately reconnect again.
        @death_queue.close
        @death_queue = Channel(String | Exception | Symbol).new
      end
    end

    # close the connection if it's not already closed and try to reconnect
    # over and over until we succeed!
    private def reconnect
      begin
        close_connection
      rescue e2 : Exception
        # p [:problem_closing, e2.message]
      end

      with_retries do
        @in_reconnecting_loop = true
        open_connection
        @in_reconnecting_loop = false
      end
    end

    private def open_connection(on_successful_connection : Proc(Nil, Nil) | Nil = nil)
      @socket = TCPSocket.new(@host, @port, 10, 10)

      # write the version and IDENTIFY directly to the socket to make sure
      # it gets to nsqd ahead of anything in the `@write_queue`
      socket_block { |socket| socket.write("  V2".to_slice) }
      @connect_timed_out = false
      @connect_completed = false
      connect_expected_completion = 5.seconds.from_now
      fiber = spawn do
        begin
          identify
          # upgrade_to_ssl_socket if @tls_v1
          next if @connect_timed_out
          start_read_loop
          start_write_loop
          @connected = true
          @last_heartbeat = Time.utc
          # we need to re-subscribe if there's a topic specified
          if @topic
            debug "Subscribing to #{@topic}"
            sub(@topic, @channel)
            re_up_ready
          end
          @connect_completed = true
        rescue Exception
          @connect_timed_out = true
        end
      end
      loop do
        if @connect_timed_out
          raise IO::TimeoutError.new
        elsif @connect_completed
          break
        else
          if connect_expected_completion > Time.utc
            sleep(0.01)
          else
            @connect_timed_out = true
            raise "unable to connect"
          end
        end
      end
      if on_successful_connection
        on_successful_connection.as(Proc(Nil, Nil)).call(nil)
      end
    end

    # closes the connection and stops listening for messages
    private def close_connection
      begin
        cls if connected?
        stop_write_loop
        @socket.as(Socket).close if @socket
        # @socket = nil
        @connected = false
      rescue e : Exception
      end
    end

    # this is called when there's a connection error in the read or write loop
    # it triggers `connect_and_monitor` to try to reconnect
    private def die(reason)
      @connected = false
      @death_queue.send(reason)
    end

    private def upgrade_to_ssl_socket
      ssl_opts = [@socket, openssl_context].compact
      @socket = OpenSSL::SSL::SSLSocket.new(*ssl_opts)
      @socket.sync_close = true
      @socket.connect
    end

    private def openssl_context
      return unless @tls_options

      context = OpenSSL::SSL::SSLContext.new
      context.cert = OpenSSL::X509::Certificate.new(File.read(@tls_options[:certificate]))
      context.key = OpenSSL::PKey::RSA.new(File.read(@tls_options[:key]))
      if @tls_options[:ca_certificate]
        context.ca_file = @tls_options[:ca_certificate]
      end
      context.verify_mode = @tls_options[:verify_mode] || OpenSSL::SSL::VERIFY_NONE
      context
    end

    # Retry the supplied block with exponential backoff.
    #
    # Borrowed liberally from:
    # https://github.com/ooyala/retries/blob/master/lib/retries.rb
    private def with_retries(&block : Int32 -> _)
      base_sleep_seconds = 0.5
      max_sleep_seconds = 15 # 30 second max sleep

      max_attempts = @connected_through_lookupd ? 10 : 100
      # Let's do this thing
      attempts = 0
      ex = nil
      while attempts < max_attempts + 2
        begin
          attempts += 1
          return block.call(attempts)
        rescue ex : Socket::Error | IO::Error | Exception
          raise ex if attempts >= max_attempts

          # The sleep time is an exponentially-increasing function of base_sleep_seconds.
          # But, it never exceeds max_sleep_seconds.
          sleep_seconds = [base_sleep_seconds * (1.2 ** (attempts - 1)), max_sleep_seconds].min
          # Randomize to a random value in the range sleep_seconds/2 .. sleep_seconds
          sleep_seconds = sleep_seconds * (0.5 * (1 + rand()))
          # But never sleep less than base_sleep_seconds
          sleep_seconds = [base_sleep_seconds, sleep_seconds].max
          # puts "failed to connect"
          warn "Failed to connect: #{ex}. Retrying in #{sleep_seconds.round(1)} seconds."

          snooze(sleep_seconds)
        rescue e : Exception
        end
      end
      if ex
        raise ex
      end
    end

    # Se we can stub for testing and reconnect in a tight loop
    private def snooze(t)
      sleep(t)
    end

    private def server_needs_rdy_re_ups?
      # versions less than 0.3.0 need RDY re-ups
      # see: https://github.com/bitly/nsq/blob/master/ChangeLog.md#030---2014-11-18
      # p @server_version
      major, minor = @server_version.as(String).split(".")[0..1].map { |v| v.gsub(/[^0-9]/, "").to_i }
      major == 0 && minor <= 2
    end

    private def validate_tls_options!
      [:key, :certificate].each do |key|
        unless @tls_options.has_key?(key)
          raise ArgumentError.new "@tls_options requires a :#{key}"
        end
      end

      [:key, :certificate, :ca_certificate].each do |key|
        if @tls_options[key] && !File.readable?(@tls_options[key])
          raise LoadError.new "@tls_options :#{key} is unreadable"
        end
      end
    end
  end
end

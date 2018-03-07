module Nsq
  class Message < Frame
    getter :attempts
    getter :id
    getter :body
    @timestamp_in_nanoseconds : UInt64
    @attempts : Int16
    @id : String
    @body : String

    def initialize(data, connection)
      super
      io = IO::Memory.new(data)

      @timestamp_in_nanoseconds = IO::ByteFormat::BigEndian.decode(UInt64, io)
      @attempts = IO::ByteFormat::BigEndian.decode(Int16, io)
      id_data = Slice(UInt8).new(16)
      io.read(id_data)
      @id = String.new(id_data)
      body_data = Slice(UInt8).new(io.size - io.pos)
      io.read(body_data)
      @body = String.new(body_data, "UTF-8")
      # p [:body, @body]
      # @timestamp_in_nanoseconds, @attempts, @id, @body = @data.unpack("Q>s>a16a*")
      # @body.force_encoding("UTF-8")
    end

    def finish
      connection.fin(@id)
    end

    def requeue(timeout = 0)
      connection.req(@id, timeout)
    end

    def touch
      connection.touch(@id)
    end

    def timestamp
      Time.at(@timestamp_in_nanoseconds / 1_000_000_000.0)
    end
  end
end

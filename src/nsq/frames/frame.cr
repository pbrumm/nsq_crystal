module Nsq
  class Frame
    include ::Nsq::AttributeLogger
    @@log_attributes = [:connection]

    getter :data
    getter :connection
    @data : String;

    def initialize(data : Slice(UInt8), connection : Connection)
      @data = String.new(data)
      @connection = connection
    end
  end
end

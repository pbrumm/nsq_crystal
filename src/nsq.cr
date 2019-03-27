require "./nsq/logger"
require "./nsq/frames/frame"
require "./nsq/frames/*"

require "./nsq/*"

module Nsq
  # TODO Put your code here
  alias Opts = Hash(Symbol, Channel(Nsq::Message | Symbol) | Array(String) | String | Int32 | Bool)
end

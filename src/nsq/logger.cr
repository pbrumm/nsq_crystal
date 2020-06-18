require "logger"

module Nsq
  @@logger = Logger.new(STDOUT)

  def self.logger
    @@logger
  end

  def self.logger=(new_logger)
    @@logger = new_logger
  end

  module AttributeLogger
    def self.included(klass)
      klass.send :class_variable_set, @@log_attributes.to_sym, Array.new(String)
    end

    def fatal(msg)
      # Nsq.logger.send(:fatal, "#{self.class.name} #{msg}")
    end

    def error(msg)
      # Nsq.logger.send(:error, "#{self.class.name} #{msg}")
    end

    def warn(msg)
      # Nsq.logger.send(:warn, "#{self.class.name} #{msg}")
    end

    def info(msg)
      # Nsq.logger.send(:info, "#{self.class.name} #{msg}")
    end

    def debug(msg)
      # Nsq.logger.send(:debug, "#{self.class.name} #{msg}")
    end

    # [:fatal, :error, :warn, :info, :debug].each do |level|
    #  define_method level do |msg|
    #    Nsq.logger.send(level, "#{prefix} #{msg}")
    #  end
    # end

    # private def prefix
    #  attrs = self.class.send(:class_variable_get, @@log_attributes.to_sym)
    #  if attrs.count > 0
    #    "[#{attrs.map{|a| "#{a.to_s}: #{self.send(a)}"}.join(' ')}] "
    #  else
    #    ""
    #  end
    # end
  end
end

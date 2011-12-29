# encoding: utf-8

require 'hot_bunnies'


module HotBunnies
  class TransportSystem
    def initialize(options={})
      @nodes = options[:nodes]
      @connection_factory = options[:connection_factory]
      @exchange_name = options[:exchange_name]
      @exchange_options = options[:exchange_options] || {}
      @message_encoder = options[:message_encoder] || Marshal.method(:dump)
      @routing_keys = options[:routing_keys]
      @routing = options[:routing] || method(:random_routing)
    end

    def connect!
      return if @connections
      @connections = @nodes.map do |node|
        @connection_factory.connect(:uri => node)
      end
    end

    def publisher
      Publisher.new(self)
    end

    def exchanges
      connect!
      @connections.map do |connection|
        channel = connection.create_channel
        channel.exchange(@exchange_name, @exchange_options.merge(:type => :direct))
      end
    end

    def encode_message(obj)
      @message_encoder.call(obj)
    end

    def select_routing_key(message)
      n = @routing.call(message)
      @routing_keys[n % @routing_keys.size]
    end

  private

    def random_routing(message)
      rand(@routing_keys.size)
    end
  end

  class Publisher
    def initialize(transport_system)
      @transport_system = transport_system
      @exchanges = @transport_system.exchanges
    end

    def publish(message)
      encoded_message = begin
        if message.is_a?(String)
        then message
        else @transport_system.encode_message(message)
        end
      end
      exchange = @exchanges.sample
      routing_key = @transport_system.select_routing_key(message)
      exchange.publish(encoded_message, :routing_key => routing_key)
    end
  end
end
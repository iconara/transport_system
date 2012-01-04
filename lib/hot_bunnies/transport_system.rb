# encoding: utf-8

require 'hot_bunnies'


module HotBunnies
  class TransportSystem
    def initialize(options={})
      @nodes = options[:nodes]
      @connection_factory = options[:connection_factory]
      @exchange_name = options[:exchange_name]
      @queue_prefix = options[:queue_prefix]
      @exchange_options = options[:exchange_options] || {}
      @queue_options = options[:queue_options] || {}
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

    def setup!
      connect!
      @exchanges = setup_exchanges!
      @queues = setup_queues!
    end

    def publisher
      Publisher.new(self)
    end

    def consumer
      Consumer.new(self)
    end

    def exchanges
      @exchanges ||= begin
        connect!
        @connections.map do |connection|
          channel = connection.create_channel
          channel.exchange(@exchange_name, :passive => true)
        end
      end
    end

    def queues
      @queues ||= begin
        connect!
        index_width = [2, Math.log10(@connections.size).to_i + 1].max
        @connections.each_with_index.map do |connection, i|
          channel = connection.create_channel
          channel.queue("#{@queue_prefix}#{i.to_s.rjust(index_width, '0')}", :passive => true)
        end
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

    def setup_exchanges!
      @connections.map do |connection|
        channel = connection.create_channel
        channel.exchange(@exchange_name, @exchange_options.merge(:type => :direct))
      end
    end

    def setup_queues!
      index_width = [2, Math.log10(@connections.size).to_i + 1].max
      @connections.each_with_index.map do |connection, i|
        channel = connection.create_channel
        queue = channel.queue("#{@queue_prefix}#{i.to_s.rjust(index_width, '0')}", @queue_options)
        @routing_keys.each_slice(@routing_keys.size/@connections.size).to_a[i].each do |rk|
          queue.bind(@exchanges[i], :routing_key => rk)
        end
        queue
      end
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

  class Consumer
    def initialize(transport_system)
      @transport_system = transport_system
      @queues = @transport_system.queues
    end

    def each(&consumer)
      @subscriptions = @queues.map { |q| q.subscribe(:ack => true) }
      @subscriptions.each { |s| s.each(:blocking => false, &consumer) }
    end

    def stop
      @subscriptions.each { |s| s.cancel }
    end
  end
end
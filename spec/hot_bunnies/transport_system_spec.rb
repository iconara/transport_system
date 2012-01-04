# encoding: utf-8

require_relative '../spec_helper'

require 'hot_bunnies/transport_system'
require 'json'


module HotBunnies
  describe TransportSystem do
    before do
      @connection_factory = stub(:connection_factory)
      @connections = []
      @channels = []
      @exchanges = []
      @queues = []
      @subscriptions = []
      3.times do |i|
        exchange = stub("exchange#{i}")
        subscription = stub("subscription#{i}")
        subscription.stub(:each)
        subscription.stub(:cancel)
        subscription.stub(:shutdown!)
        queue = stub("queue#{i}")
        queue.stub(:subscribe).with(:ack => true).and_return(subscription)
        queue.stub(:bind)
        channel = stub("channel#{i}")
        channel.stub(:exchange).and_return(exchange)
        channel.stub(:queue).and_return(queue)
        channel.stub(:close)
        connection = stub("connection#{i}")
        connection.stub(:create_channel).and_return(channel)
        connection.stub(:close)
        @connection_factory.stub(:connect).with(:uri => "amqp://mqhost0#{i}:5672").and_return(connection)
        @connections << connection
        @channels << channel
        @exchanges << exchange
        @queues << queue
        @subscriptions << subscription
      end
      @routing_keys = %w[r00 r01 r02 r03 r04 r05]
      @options = {
        :connection_factory => @connection_factory,
        :nodes => %w(amqp://mqhost00:5672 amqp://mqhost01:5672 amqp://mqhost02:5672),
        :exchange_name => 'transport_exchange',
        :routing_keys => @routing_keys,
        :queue_prefix => 'transport_queue_'
      }
      @transport_system = TransportSystem.new(@options)
    end

    describe '#connect!' do
      it 'connects to all nodes' do
        @connection_factory.should_receive(:connect).with(:uri => 'amqp://mqhost00:5672')
        @connection_factory.should_receive(:connect).with(:uri => 'amqp://mqhost01:5672')
        @connection_factory.should_receive(:connect).with(:uri => 'amqp://mqhost02:5672')
        @transport_system.connect!
      end
    end

    describe '#disconnect!' do      
      it 'closes all connections' do
        @connections.each { |c| c.should_receive(:close) }
        @transport_system.connect!
        @transport_system.disconnect!
      end

      it 'stops all consumers' do
        consumer1 = @transport_system.consumer
        consumer2 = @transport_system.consumer
        consumer1.each { |m| }
        consumer2.each { |m| }
        @transport_system.disconnect!
        consumer1.should_not be_active
        consumer2.should_not be_active
      end
    end

    describe '#setup!' do
      it 'declares the exchange on each node' do
        @channels.each { |ch| ch.should_receive(:exchange).with('transport_exchange', :type => :direct) }
        @transport_system.setup!
      end

      it 'passes along extra exchange options' do
        @options[:exchange_options] = {:durable => true}
        @transport_system = TransportSystem.new(@options)
        @channels.each { |ch| ch.should_receive(:exchange).with('transport_exchange', :type => :direct, :durable => true) }
        @transport_system.setup!
      end

      it 'declares a queue on each node' do
        @channels.each_with_index do |ch, i|
          ch.should_receive(:queue).with("transport_queue_0#{i}", anything)
        end
        @transport_system.setup!
      end

      it 'passes along extra queue options' do
        @options[:queue_options] = {:durable => true}
        @transport_system = TransportSystem.new(@options)
        @channels.each_with_index do |ch, i|
          ch.should_receive(:queue).with("transport_queue_0#{i}", :durable => true)
        end
        @transport_system.setup!
      end

      it 'binds the queues to even subsets of the routing keys' do
        @queues.each_with_index do |q, i|
          q.should_receive(:bind).with(@exchanges[i], :routing_key => "r#{(i * 2).to_s.rjust(2, '0')}")
          q.should_receive(:bind).with(@exchanges[i], :routing_key => "r#{(i * 2 + 1).to_s.rjust(2, '0')}")
        end
        @transport_system.setup!
      end

      it 'uses queue suffixes with an appropriate number of leading zeroes', :slow => true do
        @nodes = []
        @channels = []
        222.times do |i|
          index_str = i.to_s.rjust(3, '0')
          uri = "amqp://mqhost#{index_str}:5672"
          exchange = stub("exchange#{index_str}")
          queue = stub("queue#{index_str}")
          queue.stub(:bind)
          channel = stub("channel#{index_str}")
          channel.stub(:queue).and_return(queue)
          channel.stub(:exchange).and_return(exchange)
          connection = stub("connection#{index_str}")
          connection.stub(:create_channel).and_return(channel)
          @nodes << uri
          @channels << channel
          @connection_factory.stub(:connect).with(:uri => uri).and_return(connection)
        end
        @options[:nodes] = @nodes
        @options[:routing_keys] = 888.times.map { |rk| "r#{rk.to_s.rjust(3, '0')}" }
        @transport_system = TransportSystem.new(@options)
        @channels[45].should_receive(:queue).with('transport_queue_045', anything)
        @channels[111].should_receive(:queue).with('transport_queue_111', anything)
        @transport_system.setup!
      end
    end

    context 'publishing' do
      describe '#publisher' do
        it 'returns a publisher' do
          @transport_system.publisher.should_not be_nil
        end

        it 'creates a channel on each node' do
          @connections.each { |c| c.should_receive(:create_channel) }
          @transport_system.publisher
        end

        it 'passively declares the exchange on each node' do
          @channels.each { |ch| ch.should_receive(:exchange).with('transport_exchange', :passive => true) }
          @transport_system.publisher
        end
      end

      describe Publisher do
        describe '#publish' do
          before do
            srand(1337)
            @publisher = @transport_system.publisher
          end

          it 'publishes the message to a random node using a random routing key' do
            @exchanges[1].should_receive(:publish).with('hello world', :routing_key => 'r05')
            @publisher.publish('hello world')
          end

          it 'encodes a hash with the specified encoder' do
            @options[:message_encoder] = JSON.method(:generate)
            @transport_system = TransportSystem.new(@options)
            @publisher = @transport_system.publisher
            @exchanges[1].should_receive(:publish).with('{"hello":"world"}', anything)
            @publisher.publish('hello' => 'world')
          end

          it 'encodes a hash with Marshal if no encoder is specified' do
            @exchanges[1].should_receive(:publish).with("\x04\b{\x06I\"\nhello\x06:\x06ETI\"\nworld\x06;\x00T".force_encoding(Encoding::BINARY), anything)
            @publisher.publish('hello' => 'world')
          end

          it 'publishes the message with a routing key generated by a callback' do
            @options[:routing] = lambda { |msg| msg['id'].to_i(36) }
            @options[:message_encoder] = JSON.method(:generate)
            @transport_system = TransportSystem.new(@options)
            @publisher = @transport_system.publisher
            @exchanges[1].should_receive(:publish).with('{"id":"ABCDEF"}', :routing_key => 'r03')
            @publisher.publish('id' => 'ABCDEF')
          end
        end
      end
    end

    context 'consuming' do
      describe '#consumer' do
        it 'returns a consumer' do
          @transport_system.consumer.should_not be_nil
        end

        it 'creates a channel on each node' do
          @connections.each { |c| c.should_receive(:create_channel) }
          @transport_system.consumer
        end

        it 'passively declares a queue on each node' do
          @channels.each_with_index do |ch, i|
            ch.should_receive(:queue).with("transport_queue_0#{i}", :passive => true)
          end
          @transport_system.consumer
        end
      end

      describe Consumer do
        before do
          @consumer = @transport_system.consumer
        end

        describe '#each' do
          it 'subscribes to each queue' do
            received_value = nil
            block = proc { |h, m| received_value = [h, m] }
            @subscriptions.each { |s| s.should_receive(:each) { |options, &callback| callback.call('test', 'tset') } }
            @consumer.each(&block)
            received_value.should == ['test', 'tset']
          end
        end

        describe '#stop!' do
          it 'cancels all subscriptions' do
            @subscriptions.each { |s| s.should_receive(:cancel) }
            @consumer.each { |m| }
            @consumer.stop!
          end
        end

        describe '#active?' do
          it 'returns false initially' do
            @consumer.should_not be_active
          end

          it 'returns true if there is an active subscription' do
            @consumer.each { |m| }
            @consumer.should be_active
          end

          it 'returns false when stopped' do
            @consumer.each { |m| }
            @consumer.stop!
            @consumer.should_not be_active
          end
        end
      end
    end
  end
end
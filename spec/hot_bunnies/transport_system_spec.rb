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
      3.times do |i|
        exchange = stub("exchange#{i}")
        channel = stub("channel#{i}")
        channel.stub(:exchange).and_return(exchange)
        connection = stub("connection#{i}")
        connection.stub(:create_channel).and_return(channel)
        @connection_factory.stub(:connect).with(:uri => "amqp://mqhost0#{i}:5672").and_return(connection)
        @connections << connection
        @channels << channel
        @exchanges << exchange
      end
      @options = {
        :connection_factory => @connection_factory,
        :nodes => %w(amqp://mqhost00:5672 amqp://mqhost01:5672 amqp://mqhost02:5672),
        :exchange_name => 'transport_exchange',
        :routing_keys => %w[r00 r01 r02 r03 r04 r05],
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

    context 'publishing' do
      describe '#publisher' do
        it 'returns a publisher' do
          @transport_system.publisher.should_not be_nil
        end

        it 'creates a channel on each node' do
          @connections.each { |c| c.should_receive(:create_channel) }
          @transport_system.publisher
        end

        it 'declares the exchange on each node' do
          @channels.each { |ch| ch.should_receive(:exchange).with('transport_exchange', :type => :direct) }
          @transport_system.publisher
        end

        it 'passes along extra exchange options' do
          @options[:exchange_options] = {:durable => true}
          @transport_system = TransportSystem.new(@options)
          @channels.each { |ch| ch.should_receive(:exchange).with('transport_exchange', :type => :direct, :durable => true) }
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

          it 'publishes the message with a routing key based on a property' do
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
  end
end
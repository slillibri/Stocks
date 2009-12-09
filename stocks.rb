#! /usr/bin/env ruby

require 'rubygems'
require 'amqp'
require 'mq'
require 'pp'
require 'net/http'
require 'cassandra'

include Cassandra::Constants

class StockWatcher
  trap("INT") do
    puts "Stopping Watcher"
    EM.stop_event_loop
  end
  
  def initialize args
    @stock = args[:stock]
    @supercol = args[:super]
  end
  def run
    AMQP.start(:host => 'localhost', :logging => false) do
      cas = Cassandra.new('Stocks')
      queue = MQ.queue("#{@stock} stock")
      queue.bind(MQ.topic('stock_quotes'), :key => "stock.quote.#{@stock}")
      queue.subscribe do |headers,msg|
        result = YAML::load(msg)
        key = Date.parse(result['last_trade']).to_s
        cas.insert(@supercol.to_sym, key, {UUID.new.to_s => result})
        puts "Inserted #{key} into #{@supercol}"
      end 
    end    
  end
end

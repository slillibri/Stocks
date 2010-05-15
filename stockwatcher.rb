#! /usr/bin/env ruby

require 'rubygems'
require 'amqp'
require 'mq'
require 'pp'
require 'net/http'
require 'cassandra'
require 'uuid'
require 'log4r'

include Cassandra::Constants

class StockWatcher
  trap("INT") do
    puts "Stopping Watcher"
    EM.stop_event_loop
  end
  
  def initialize args
    @stock = args[:stock]
    @supercol = args[:super]
    
    ##Setup the logger
    loggertype = self.class.to_s.downcase
    @logger = Log4r::Logger.new("#{loggertype}")
    @logger.trace = true
    o = Log4r::FileOutputter.new("#{loggertype}", :filename => "#{@stock}.log", :trunc => false)
    o.formatter = Log4r::BasicFormatter
    @logger.outputters = o
    @logger.level = DEBUG
    @logger.debug("Initialization complete")
  end
  def run
    AMQP.start(:host => 'localhost', :logging => false) do
      cas = Cassandra.new('Stocks')
      queue = MQ.queue("#{@stock} stock")
      queue.bind(MQ.topic('stock_quotes'), :key => "stock.quote.#{@stock}")
      queue.subscribe do |headers,msg|
        begin
          result = YAML::load(msg)
          key = Date.parse(result['last_trade']).to_s
          cas.insert(@supercol.to_sym, key, {UUID.new.to_s => result})
          @logger.debug("Inserted #{key} into #{@supercol}")
        rescue Exception => e
          @logger.error("Failed inserting into cassandra. #{e}")
          retry
        end
      end 
    end    
  end
end

companies = [{:stock => 'amzn', :super => 'Amazon'},
             {:stock => 'aapl', :super => 'Apple'},
             {:stock => 'goog', :super => 'Google'},
             {:stock => 'msft', :super => 'Microsoft'}]

companies.each do |company|
  puts "Starting #{company[:super]}"
  orig_stdout = $stdout
  $stdout = File.new('/dev/null', 'w')
  pid = fork do
    $0 = "stockwatcher.rb (#{company[:super]})"
    s = StockWatcher.new(company)
    s.run
  end
  ::Process.detach pid
  $stdout = orig_stdout
  puts "#{company[:super]} started"
end

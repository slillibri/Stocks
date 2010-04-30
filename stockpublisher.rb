#! /usr/bin/env ruby

require 'rubygems'
require 'amqp'
require 'mq'
require 'pp'
require 'net/http'
require 'time'

class StockPublisher
  trap("INT") do
    puts "Stopping Publisher"
    MQ.close
    EM.stop_event_loop
  end

  def run
    EM.run do
      connection = AMQP.connect(:host => 'localhost', :logging => false)
      channel = MQ.new(connection)
      exchange = MQ::Exchange.new(channel, :topic, 'stock_quotes', :durable => true)
      EM.add_periodic_timer(30) do
        %w[aapl msft goog amzn].each do |stock|
          ## Check if the market is actually open (no need to track after market) Market is open 9:30 - 16:00
          ## Probably should check holiday someday ## Time on the server is UTC
          day = Date.today()
          if (day.wday > 0 && day.wday < 6)
            time = Time.now()
            dst = 'EST'
            if (time.dst?) 
              ## Fall back if they have sprung forward
              time = time - (60 * 60) 
              dst = 'EDT'
            end
            ## If it is past 2:30pm(UTC) and before 21:00(UTC)
            start = Time.utc(day.year,day.mon,day.day,14,30,00)
            endtime = Time.utc(day.year,day.mon,day.day,21,00,00)
            
            if (time >= start && time <= endtime)
              queue = MQ.queue("#{stock} stock", :durable => true).bind(exchange, :key => "stock.quote.#{stock}")
              result = fetchStock(stock)
              last_time = Time.parse("#{result['last_date']} #{result['last_time']} #{dst}").to_i
              result['time'] = "\"#{last_time.to_s}\""
              result = YAML::dump(result)
              
              exchange.publish(result, :routing_key => "stock.quote.#{stock}", :persistent => true)
              puts "Published #{stock.upcase} stock information"
            end
          end
        end
      end
    end
  end
  
  def fetchStock(symbol)
    uri = URI.parse("http://download.finance.yahoo.com/d/quotes.csv?s=#{symbol}&f=sl1d1t1c1ohgv&e=.csv")
    res = Net::HTTP.start(uri.host, 80) do |http|
      http.read_timeout = 30
      http.get("http://download.finance.yahoo.com/d/quotes.csv?s=#{symbol}&f=sl1d1t1c1ohgv&e=.csv")          
    end
    msg = res.body.split(',')
    resultHash = {}
    keys = %w[symbol price last_trade last_time change open day_high day_low volume]
    msg.each {|atom| resultHash[keys[msg.index(atom)]] = atom}
    resultHash
  end
  
end

pid = fork do
  puts "Starting StockPublisher"
  orig_stdout = $stdout
  $stdout = File.new('/dev/null', 'w')
  $0 = 'stockpublisher.rb'
  s = StockPublisher.new()
  s.run
  $stdout = orig_stdout
  puts "StockPublisher started"
end

::Process.detach pid

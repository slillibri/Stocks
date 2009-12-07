#! /usr/bin/env ruby

require 'rubygems'
require 'amqp'
require 'mq'
require 'pp'
require 'net/http'

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
          ## Probably should check holiday someday
          day = Date.now()
          if (day.wday > 0 && day.wday < 6)
            time = Time.now()
          
            if (time.dst?) 
              time = time + (60 * 60) 
            end
          
            if ((time.hour >= 9 && time.minute > 30) && (time.hour < 16)) 
              queue = MQ.queue("#{stock} stock", :durable => true).bind(exchange, :key => "stock.quote.#{stock}")
              result = YAML::dump(fetchStock(stock))
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

s = StockPublisher.new()
s.run
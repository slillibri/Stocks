#! /usr/bin/env ruby

require 'rubygems'
require 'getoptlong'
require 'cassandra'
require 'gruff'
require 'time'
require 'pp'

opts = GetoptLong.new(
          ['--symbol', '-s', GetoptLong::REQUIRED_ARGUMENT],
          ['--date', '-d', GetoptLong::REQUIRED_ARGUMENT],
          ['--graph', '-g', GetoptLong::OPTIONAL_ARGUMENT],
          ['--debug', '-v', GetoptLong::NO_ARGUMENT])
          
symbol = nil
date = []
graph = 'graph.png'
$debug = false

begin
  opts.each do |opt,arg|
    case opt
    when '--symbol'
      symbol = arg.capitalize.to_sym
    when '--date'
      date.push(arg)
    when '--graph'
      unless arg == ''
        graph = arg
      end
    when '--debug'
      $debug = true
    end
  end
rescue
  opts.error_message()
  exit(1)
end

unless symbol
  puts "Missing --symbol argument"
  exit(1)
end

unless date.size
  puts "Missing one or more --date arguments"
  exit(1)
end

def fetchStocks(stock, key)
  cas = Cassandra.new('Stocks')
  stocks = cas.get(stock, key, :count => 900).values.sort {|a,b| Time.parse(a['last_time']) <=> Time.parse(b['last_time'])}
  values = []
  stocks.map {|val| values.push(val['price'].to_f)}
  if $debug
    pp values
  end
  values
end

def writeGraph(stock, key, graph, values)
  gruff = Gruff::Line.new
  gruff.title = "#{stock.to_s}"
  
  values.each do |key,value|
    gruff.data("#{key}", value)
  end
  
  gruff.write(graph)
end

values = {}
date.each do |key|
  values[key] = fetchStocks(symbol, key)
end

writeGraph(symbol, date, graph, values)
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
          ['--num_days', '-n', GetoptLong::REQUIRED_ARGUMENT],
          ['--graph', '-g', GetoptLong::OPTIONAL_ARGUMENT],
          ['--debug', '-v', GetoptLong::NO_ARGUMENT])

conf = {:graph => 'graph.png'}          
symbol = nil
date = []
graph = 'graph.png'
$debug = false

begin
  opts.each do |opt,arg|
    case opt
    when '--symbol'
      conf[:symbol] = arg.capitalize.to_sym
    when '--date'
      conf[:date] = arg
    when '--graph'
      unless arg == ''
        conf[:graph] = arg
      end
    when '--debug'
      $debug = true
    when '--num_days'
      conf[:days] = arg.to_i
    end
  end
rescue
  opts.error_message()
  exit(1)
end

unless conf[:symbol]
  puts "Missing --symbol argument"
  exit(1)
end

unless conf[:date]
  puts "Missing --date argument"
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
#date.each do |key|
conf[:days].times do |day|
  date = DateTime.parse(conf[:date]) - day
  key = date.strftime('%Y-%m-%d')
  if $debug
    puts "Fetching stocks for #{key}"
  end
  values[key] = fetchStocks(conf[:symbol], key)
end

writeGraph(conf[:symbol], conf[:date], conf[:graph], values)
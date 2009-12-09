#! /usr/bin/env ruby

require "rubygems"
require "cassandra"
require "gruff"

def select_prices(arr)
  tmp_arr = []
  arr.each do |a|
    tmp_arr.push a['price'].to_f if a['last_time'] != '"4:00pm"'
  end
  tmp_arr
end

cas = Cassandra.new('Stocks')
aapl = cas.get(:Apple, '2009-12-08', :count => 2880)
aapl2 = cas.get(:Apple, '2009-12-07', :count => 2880)

aapl = aapl.values.sort {|a,b| a['last_time'] <=> b['last_time']}
aapl2 = aapl2.values.sort {|a,b| a['last_time'] <=> b['last_time']}

aapl = select_prices(aapl)
aapl2 = select_prices(aapl2)

graph = Gruff::Line.new(800)
graph.hide_dots
graph.title = 'Tracked Stocks 2009-12-08'

graph.data('Apple 2009-12-08', aapl)
graph.data('Apple 2009-12-07', aapl2)

graph.write('stocks.png')
#! /usr/bin/env ruby

require 'rubygems'
require 'date'
require 'cassandra'

cas = Cassandra.new('Stocks')
if(ARGV[0])  
  key = Date.parse(ARGV[0])
else
  key = Date.today
end

columns = [:Google, :Apple, :Microsoft, :Amazon]

columns.each do |col|
  results = cas.get(col, key.strftime, :count => 2880)
  puts "Column: #{col.to_s}, Result-size: #{results.count}"
end


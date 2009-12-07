#! /usr/bin/env ruby

require 'rubygems'
require 'date'
require 'cassandra'

cas = Cassandra.new('Stocks')
key = Date.today.to_s
columns = [:Google, :Apple, :Microsoft, :Amazon]

columns.each do |col|
  results = cas.get(col, key, :count => 2880)
  puts "Column: #{col.to_s}, Result-size: #{results.count}"
end


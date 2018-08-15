#!/usr/bin/env ruby

require_relative 'error'

require 'bunny'

args = build_args
errors = validate_args(args)
Error.exit_with_errors(errors) unless errors.empty?

session = Bunny.new("amqp://guest:guest@localhost:5672")
connection = session.start

queue = create_queue(connection, args[:topic], args[:queue], args[:routing_key])
queue.subscribe(block: true) do |delivery_info, properties, payload|
    puts "Received event: \n\tpayload: #{payload}\n\tproperties: #{properties}\n\tdelivery info: #{delivery_info}"
end

connection.close

BEGIN {

def create_queue(connection, topic, queue, routing_key)
  channel = connection.create_channel
  exchange = channel.topic(topic, durable: true)
  channel.queue(queue, durable: true).bind(exchange, routing_key: routing_key)
end

def validate_args(args)
  errors = []
  errors << 'An event topic is needed.' unless args[:topic]
  errors
end

def build_args
  {
    topic: ARGV[0],
    queue: ARGV[1] || 'events.consumer',
    routing_key: ARGV[2] || '#'
  }
end

}


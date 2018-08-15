#!/usr/bin/env ruby

require 'time'
require 'bunny'
require 'multi_json'
require 'securerandom'

RUN_EXAMPLE = './publisher.rb events/<event_name>.json'

unless ARGV[0]
  Error.exit_with_errors(["No event given.\n\nUsage: #{RUN_EXAMPLE}"])
end
Error.exit_with_errors(['File not found!']) unless File.exist?(ARGV[0])

begin
  args = build_args
rescue Errno::ENOENT
  Error.exit_with_errors(['Missing event file given!'])
end

errors = validate_args(args)
Error.exit_with_errors(errors) unless errors.empty?

session = Bunny.new("amqp://guest:guest@localhost:5672")
connection = session.start

publish(connection, event_metadata(
    args[:resource],
    args[:system],
    args[:event_name],
  ).merge(**args[:event])
)

session.close

BEGIN {

def publish(connection, event)
  system = event[:system]

  exchange = create_exchange(connection, system)
  exchange.publish(
    event.to_json, :routing_key => "#{system}.#{event[:resource]}"
  )

  puts "Emitted #{event}"
end

def create_exchange(conn, system)
  ch = conn.create_channel
  ch.topic(system, durable: true)
end

def event_metadata(resource, system, event_name)
  {
    platform_tid: SecureRandom.uuid,
    event_id: SecureRandom.uuid,
    timestamp: Time.new.utc.iso8601,
    system: system,
    resource: resource,
    event: event_name,
  }
end

def build_args
  event_data = MultiJson.load(File.open(ARGV[0], 'r').read, symbolize_keys: true)

  {
    event: event_data[:event],
    event_name: ARGV[0].match(/(\w+)(\/)(\w+)([.]\w+)/)[3],
    system: event_data[:system],
    resource: event_data[:resource],
  }
end

def validate_args(args)
  errors = []
  errors << 'missing event resource.' unless args[:resource]
  errors << 'missing event system.' unless args[:system]
  errors << 'missing event name.' unless args[:event_name]
  errors << 'missing event payload.' unless args[:event]
  errors
end

}


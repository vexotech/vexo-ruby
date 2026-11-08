# frozen_string_literal: true

# Conformance driver: reads a scenario on stdin, drives the SDK, and prints JSON lines.
#
# Protocol: vexo-sdk-conformance/README.md. Run: python3 run.py --driver "ruby conformance/driver.rb"

require "json"
require "time"
require_relative "../lib/vexo"

# Scenario option => [keyword, divisor to the SDK's unit].
OPTIONS = { "flushAt" => [:flush_at, 1], "flushIntervalMs" => [:flush_interval, 1000.0],
            "maxRetries" => [:max_retries, 1], "maxQueueSize" => [:max_queue_size, 1],
            "timeoutMs" => [:timeout, 1000.0], "shutdownTimeoutMs" => [:shutdown_timeout, 1000.0] }.freeze
LOCK = Mutex.new # on_error runs on the sending thread too
$stdout.sync = true

def output(line)
  LOCK.synchronize { $stdout.puts(JSON.generate(line)) }
end

def monotonic
  Process.clock_gettime(Process::CLOCK_MONOTONIC)
end

scenario = JSON.parse($stdin.read)
options = OPTIONS.select { |key, _| scenario["options"].key?(key) }
                 .to_h { |key, (name, divisor)| [name, scenario["options"][key] / divisor] }
on_error = ->(error) { output(type: "error", message: error.message, status: error.status) }
vexo = Vexo::Client.new(scenario["apiKey"], host: scenario["host"], on_error: on_error, **options)

scenario["steps"].each do |step|
  started = monotonic
  case step["op"]
  when "capture"
    properties = step["properties"]
    properties = (properties || {}).merge("poison" => Float::NAN) if step["poison"]
    timestamp = step.key?("at") ? Time.iso8601(step["at"]) : step["timestamp"]
    vexo.capture(distinct_id: step["distinctId"], event: step["event"], properties: properties, timestamp: timestamp,
                 message_id: step["messageId"])
  when "identify" then vexo.identify(distinct_id: step["distinctId"], traits: step["traits"])
  when "flush" then vexo.flush
  when "shutdown" then vexo.shutdown
  when "sleep" then sleep(step["ms"] / 1000.0)
  when "exit" then exit!(0)
  end
  output(type: "step", op: step["op"], ms: (monotonic - started) * 1000)
end

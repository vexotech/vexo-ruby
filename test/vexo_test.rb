# frozen_string_literal: true

require "minitest/autorun"
require "date"
require "json"
require "rbconfig"
require "socket"
require "vexo"

# A local API that records each batch request and answers with the next scripted response: [status, headers, body]
# (a String body is sent as is), :close (close the connection without an answer), or :hang (never answer).
class FakeApi
  attr_reader :requests

  def initialize(responses = [])
    @responses = responses.dup
    @requests = []
    @sockets = []
    @lock = Mutex.new
    @server = TCPServer.new("127.0.0.1", 0)
    Thread.new do
      loop { Thread.new(@server.accept) { |socket| handle(socket) } }
    rescue IOError, SystemCallError
      nil
    end
  end

  def host
    "http://127.0.0.1:#{@server.addr[1]}"
  end

  def close
    @server.close
    @lock.synchronize { @sockets.each(&:close) }
  end

  private

  def handle(socket)
    @lock.synchronize { @sockets << socket }
    path = socket.gets.split[1]
    headers = {}
    while (line = socket.gets) && line != "\r\n"
      name, value = line.split(":", 2)
      headers[name.downcase] = value.strip
    end
    raw = socket.read(headers["content-length"].to_i)
    body = JSON.parse(raw)
    response = @lock.synchronize do
      @requests << { path: path, headers: headers, bytes: raw.bytesize, body: body }
      @responses.shift
    end
    return if response == :hang
    return socket.close if response == :close

    status, extra, payload = response || [200, {}, nil]
    payload ||= { success: true, code: 200, data: { accepted: body["batch"].size, dropped: [] } }
    data = payload.is_a?(String) ? payload : JSON.generate(payload)
    extra = extra.map { |name, value| "#{name}: #{value}\r\n" }.join
    socket.write("HTTP/1.1 #{status} Scripted\r\nContent-Type: application/json\r\nContent-Length: #{data.bytesize}\r\n" \
                 "Connection: close\r\n#{extra}\r\n#{data}")
    socket.close
  rescue IOError, SystemCallError
    nil
  end
end

class VexoTest < Minitest::Test
  LIB = File.expand_path("../lib", __dir__)
  MAX_BODY_BYTES = 5 * 1024 * 1024
  NOON = Time.utc(2026, 11, 7, 12)
  NOON_MS = 1_794_052_800_000

  def setup
    @cleanups = []
    @errors = []
  end

  def teardown
    @cleanups.reverse_each(&:call)
  end

  def client(responses = [], **options)
    @api = FakeApi.new(responses)
    @cleanups << @api.method(:close)
    vexo = Vexo::Client.new("test-key", **{ host: @api.host, flush_interval: 60, on_error: @errors.method(:<<) }
                                         .merge(options))
    @cleanups << vexo.method(:shutdown)
    vexo
  end

  def events(request)
    @api.requests[request][:body]["batch"].map { |message| message["event"] }
  end

  def wait_for_requests(count, seconds = 2)
    deadline = monotonic + seconds
    sleep(0.01) while @api.requests.size < count && monotonic < deadline
    assert_equal count, @api.requests.size
  end

  def monotonic
    Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end

  # A port that refuses connections.
  def unreachable_host
    server = TCPServer.new("127.0.0.1", 0)
    port = server.addr[1]
    server.close
    "http://127.0.0.1:#{port}"
  end

  def ruby(script, host)
    IO.popen([RbConfig.ruby, "-I", LIB, "-e", script, host], err: %i[child out], &:read)
  end

  def test_sends_queued_messages_as_one_batch_with_the_api_key
    vexo = client
    vexo.identify(distinct_id: "user_1", traits: { plan: "pro" })
    vexo.capture(distinct_id: "user_1", event: "purchase", properties: { amount: 42 }, timestamp: NOON)
    vexo.shutdown

    assert_equal 1, @api.requests.size
    request = @api.requests[0]
    assert_equal "/v1/batch", request[:path]
    assert_equal "test-key", request[:headers]["x-api-key"]
    assert_equal "application/json", request[:headers]["content-type"]
    assert_equal "vexo-ruby/#{Vexo::VERSION}", request[:headers]["user-agent"]
    identify, capture = request[:body]["batch"]
    assert_equal({ "type" => "identify", "distinctId" => "user_1", "traits" => { "plan" => "pro" } },
                 identify.reject { |key| key == "messageId" })
    assert_equal({ "type" => "capture", "distinctId" => "user_1", "event" => "purchase",
                   "properties" => { "amount" => 42 }, "timestamp" => NOON_MS },
                 capture.reject { |key| key == "messageId" })
    assert_match(/\A\h{8}-\h{4}-4\h{3}-\h{4}-\h{12}\z/, identify["messageId"])
    refute_equal identify["messageId"], capture["messageId"]
    assert_empty @errors
  end

  def test_sends_timestamps_as_integer_epoch_milliseconds
    vexo = client
    before = Process.clock_gettime(Process::CLOCK_REALTIME, :millisecond)
    vexo.capture(distinct_id: "u", event: "now")
    vexo.capture(distinct_id: "u", event: "float", timestamp: NOON_MS + 0.9)
    vexo.capture(distinct_id: "u", event: "offset", timestamp: Time.new(2026, 11, 7, 14, 0, 0, "+02:00"))
    vexo.capture(distinct_id: "u", event: "date_time", timestamp: DateTime.new(2026, 11, 7, 12))
    vexo.capture(distinct_id: "u", event: "date", timestamp: Date.new(2026, 11, 7))
    vexo.capture(distinct_id: "u", event: "iso", timestamp: "2026-11-07T12:00:00Z")
    vexo.flush

    now, float, offset, date_time, date, iso = @api.requests[0][:body]["batch"].map { |message| message["timestamp"] }
    assert_kind_of Integer, now
    assert_operator now, :>=, before
    assert_equal [NOON_MS] * 3, [float, offset, date_time]
    assert_equal NOON_MS - (12 * 3600 * 1000), date
    assert_equal "2026-11-07T12:00:00Z", iso
  end

  def test_reports_bad_input_instead_of_raising
    vexo = client
    vexo.capture(event: "no_distinct_id")
    vexo.capture(distinct_id: "  ", event: "blank_distinct_id")
    vexo.capture(distinct_id: "u")
    vexo.identify
    vexo.capture(distinct_id: "u", event: "nan_timestamp", timestamp: Float::NAN)
    vexo.capture(distinct_id: "u", event: "symbol_timestamp", timestamp: :yesterday)
    vexo.capture(distinct_id: "u", event: "ok")
    vexo.flush

    assert_equal [["ok"]], @api.requests.map { |request| request[:body]["batch"].map { |m| m["event"] } }
    assert_equal 6, @errors.size
    assert(@errors.all?(Vexo::Error))
  end

  def test_an_on_error_that_raises_never_reaches_the_caller
    vexo = client(on_error: ->(_error) { raise "boom" })
    assert_output(nil, /on_error raised RuntimeError: boom/) { assert_nil vexo.capture(event: "no_distinct_id") }
  end

  def test_rejects_messages_after_shutdown
    vexo = client
    vexo.shutdown
    vexo.capture(distinct_id: "u", event: "late")
    vexo.flush

    assert_empty @api.requests
    assert_match(/shut down/, @errors[0].message)
  end

  def test_uses_the_message_id_you_pass
    vexo = client
    vexo.capture(distinct_id: "u", event: "a", message_id: "order-42")
    vexo.flush

    assert_equal "order-42", @api.requests[0][:body]["batch"][0]["messageId"]
  end

  def test_sends_values_json_cannot_encode_as_strings
    vexo = client
    vexo.capture(distinct_id: "u", event: "e", properties: { at: NOON, on: Date.new(2026, 1, 2), plan: :pro })
    vexo.flush

    assert_equal({ "at" => "2026-11-07 12:00:00 UTC", "on" => "2026-01-02", "plan" => "pro" },
                 @api.requests[0][:body]["batch"][0]["properties"])
  end

  def test_drops_only_the_message_that_cannot_be_serialized
    vexo = client
    circular = {}
    circular[:self] = circular
    vexo.capture(distinct_id: "u", event: "ok")
    vexo.capture(distinct_id: "u", event: "circular", properties: circular)
    vexo.capture(distinct_id: "u", event: "nan", properties: { amount: Float::NAN })
    vexo.capture(distinct_id: "u", event: "infinity", properties: { nested: [{ amount: -Float::INFINITY }] })
    vexo.capture(distinct_id: "u", event: "bad_utf8", properties: { name: (+"\xff").force_encoding(Encoding::UTF_8) })
    vexo.capture(distinct_id: "u", event: "ok_too")
    vexo.flush

    assert_equal [%w[ok ok_too]], @api.requests.map { |request| request[:body]["batch"].map { |m| m["event"] } }
    assert_equal 4, @errors.size
    assert(@errors.all? { |error| error.message.start_with?("vexo: message dropped") })
  end

  def test_flushes_when_the_queue_reaches_flush_at
    vexo = client(flush_at: 2)
    vexo.capture(distinct_id: "u", event: "a")
    vexo.capture(distinct_id: "u", event: "b")

    wait_for_requests(1)
    assert_equal %w[a b], events(0)
  end

  def test_flushes_on_the_interval
    client(flush_interval: 0.05).capture(distinct_id: "u", event: "a")

    wait_for_requests(1)
  end

  def test_sends_at_most_500_messages_in_a_request
    vexo = client(flush_at: 1000)
    600.times { |i| vexo.capture(distinct_id: "u", event: "e#{i}") }
    vexo.flush

    sizes = @api.requests.map { |request| request[:body]["batch"].size }
    assert_equal 500, sizes.max
    assert_equal 600, sizes.sum
  end

  def test_splits_batches_so_each_request_body_stays_under_5_mb
    vexo = client
    blob = "x" * (2 * 1024 * 1024)
    3.times { |i| vexo.capture(distinct_id: "u", event: "e#{i}", properties: { blob: blob }) }
    vexo.capture(distinct_id: "u", event: "too_big", properties: { blob: "x" * MAX_BODY_BYTES })
    vexo.flush

    assert_equal [%w[e0 e1], %w[e2]], [events(0), events(1)]
    assert(@api.requests.all? { |request| request[:bytes] <= MAX_BODY_BYTES })
    assert_equal ["vexo: message dropped, it is larger than 5 MB"], @errors.map(&:message)
  end

  def test_fills_a_request_up_to_exactly_5_mb
    vexo = client
    vexo.capture(distinct_id: "u", event: "e", properties: { blob: "" }, timestamp: NOON)
    vexo.flush
    envelope = '{"batch":[]}'.bytesize
    message_bytes = @api.requests[0][:bytes] - envelope
    # The blob bytes two messages can carry so that they, the comma between them, and the envelope come to 5 MB.
    room = MAX_BODY_BYTES - envelope - 1 - (2 * message_bytes)

    [room, room + 1].each do |blob_bytes|
      vexo.capture(distinct_id: "u", event: "e", properties: { blob: "x" * (blob_bytes / 2) }, timestamp: NOON)
      vexo.capture(distinct_id: "u", event: "e", properties: { blob: "x" * (blob_bytes - (blob_bytes / 2)) }, timestamp: NOON)
      vexo.flush
    end

    assert_equal [1, 2, 1, 1], @api.requests.map { |request| request[:body]["batch"].size }
    assert_equal MAX_BODY_BYTES, @api.requests[1][:bytes]
    assert_empty @errors
  end

  def test_drops_messages_once_the_queue_is_full
    vexo = client(max_queue_size: 1)
    vexo.capture(distinct_id: "u", event: "a")
    vexo.capture(distinct_id: "u", event: "b")
    vexo.flush

    assert_equal %w[a], events(0)
    assert_equal ["vexo: the queue is full; message dropped"], @errors.map(&:message)
  end

  def test_retries_503_and_429_with_the_same_message_id_and_honors_retry_after
    vexo = client([[503, { "Retry-After" => "0" }, { success: false }], [429, { "Retry-After" => "0" }, { success: false }]])
    vexo.capture(distinct_id: "u", event: "a")
    started = monotonic
    vexo.flush

    ids = @api.requests.map { |request| request[:body]["batch"][0]["messageId"] }
    assert_equal 3, ids.size
    assert_equal 1, ids.uniq.size
    # Retry-After: 0 replaces the backoff, which waits at least 0.5 s before the first retry.
    assert_operator monotonic - started, :<, 0.5
    assert_empty @errors
  end

  def test_caps_retry_after_at_60_seconds
    vexo = client

    assert_equal [60.0, 2.5, 0.0, nil, nil], ["3600", "2.5", "-1", "Wed, 21 Oct 2026 07:28:00 GMT", nil].map { |header|
      vexo.send(:retry_after, header)
    }
  end

  def test_backs_off_exponentially_with_jitter_up_to_30_seconds
    vexo = client

    50.times do
      assert_includes 0.5..1.0, vexo.send(:backoff, 0)
      assert_includes 4.0..8.0, vexo.send(:backoff, 3)
      assert_includes 15.0..30.0, vexo.send(:backoff, 10)
    end
  end

  def test_retries_a_network_error_with_the_same_batch
    vexo = client([:close])
    vexo.capture(distinct_id: "u", event: "a")
    vexo.flush

    assert_equal 2, @api.requests.size
    assert_equal @api.requests[0][:body], @api.requests[1][:body]
    assert_empty @errors
  end

  def test_does_not_retry_a_4xx_and_reports_it
    [400, 401, 406, 413].each do |status|
      @errors.clear
      vexo = client([[status, {}, { success: false, message: "Rejected" }]])
      vexo.capture(distinct_id: "u", event: "a")
      vexo.flush

      assert_equal 1, @api.requests.size
      assert_equal [status], @errors.map(&:status)
      assert_equal %w[a], @errors[0].batch.map { |message| message["event"] }
    end
  end

  def test_gives_up_after_max_retries
    vexo = client([[500, { "Retry-After" => "0" }, {}]] * 5, max_retries: 2)
    vexo.capture(distinct_id: "u", event: "a")
    vexo.flush

    assert_equal 3, @api.requests.size
    assert_equal [500], @errors.map(&:status)
  end

  def test_reports_the_messages_the_server_dropped_without_resending_them
    dropped = { success: true, code: 200, data: { accepted: 1, dropped: [{ index: 1, reason: "too big" }], failed: [] } }
    vexo = client([[200, {}, dropped]])
    vexo.capture(distinct_id: "u", event: "ok")
    vexo.capture(distinct_id: "u", event: "too_big")
    vexo.flush

    assert_equal 1, @api.requests.size
    assert_equal [{ message: @api.requests[0][:body]["batch"][1], reason: "too big" }], @errors[0].dropped
  end

  def test_sends_again_only_the_messages_the_server_reports_as_failed
    failed = { success: true, code: 200, data: { accepted: 1, dropped: [], failed: [{ index: 1, reason: "retry" }] } }
    vexo = client([[200, {}, failed]])
    vexo.capture(distinct_id: "a", event: "stored")
    vexo.capture(distinct_id: "b", event: "failed_once")
    vexo.flush

    assert_equal [%w[stored failed_once], %w[failed_once]], [events(0), events(1)]
    assert_equal @api.requests[0][:body]["batch"][1]["messageId"], @api.requests[1][:body]["batch"][0]["messageId"]
    assert_empty @errors
  end

  def test_reports_failed_messages_that_still_fail_after_the_retries
    failed = { success: true, code: 200, data: { accepted: 0, dropped: [], failed: [{ index: 0, reason: "retry" }] } }
    vexo = client([[200, {}, failed]] * 2, max_retries: 1)
    vexo.capture(distinct_id: "u", event: "a")
    vexo.flush

    assert_equal 2, @api.requests.size
    assert_match(/could not store 1 message/, @errors[0].message)
    assert_equal "a", @errors[0].dropped[0][:message]["event"]
  end

  def test_gives_later_batches_one_attempt_until_a_request_succeeds
    error = [500, { "Retry-After" => "0" }, {}]
    ok = [200, {}, nil]
    vexo = client([error, error, error, error, ok, error, ok], flush_at: 1, max_retries: 2)
    4.times { |i| vexo.capture(distinct_id: "u", event: "e#{i}") }
    vexo.flush

    # e0 uses up its retries, e1 gets one attempt, e2 succeeds and closes the circuit, so e3 is retried again.
    assert_equal %w[e0 e0 e0 e1 e2 e3 e3], @api.requests.each_index.flat_map { |i| events(i) }
    assert_equal [%w[e0], %w[e1]], @errors.map { |e| e.batch.map { |message| message["event"] } }
  end

  def test_shutdown_gives_up_at_shutdown_timeout_when_the_api_is_unreachable
    vexo = Vexo::Client.new("test-key", host: unreachable_host, flush_at: 1, flush_interval: 60, timeout: 0.5,
                                        shutdown_timeout: 1, on_error: @errors.method(:<<))
    5.times { |i| vexo.capture(distinct_id: "u", event: "e#{i}") }
    started = monotonic
    vexo.shutdown

    assert_operator monotonic - started, :<, 1.6
    assert(@errors.any? { |error| error.message.include?("deadline") })
  end

  def test_shutdown_aborts_the_request_in_flight_at_shutdown_timeout
    vexo = client([:hang] * 3, flush_at: 1, timeout: 30, shutdown_timeout: 1)
    3.times { |i| vexo.capture(distinct_id: "u", event: "e#{i}") }
    wait_for_requests(1)
    started = monotonic
    vexo.shutdown

    assert_operator monotonic - started, :<, 1.6
    assert_match(/shutdown deadline reached/, @errors[0].message)
    # e0 was in flight on the background thread; e1 and e2 were still queued.
    assert_equal [%w[e0], %w[e1 e2]], @errors.map { |error| error.batch.map { |message| message["event"] } }
  end

  def test_shutdown_aborts_a_backoff_wait_at_shutdown_timeout
    vexo = client([[503, { "Retry-After" => "30" }, {}]], flush_at: 1, shutdown_timeout: 1)
    vexo.capture(distinct_id: "u", event: "a")
    wait_for_requests(1)
    started = monotonic
    vexo.shutdown

    assert_operator monotonic - started, :<, 1.6
    assert_equal [%w[a]], @errors.map { |error| error.batch.map { |message| message["event"] } }
  end

  def test_shutdown_is_safe_to_call_again
    vexo = client
    vexo.capture(distinct_id: "u", event: "a")
    vexo.shutdown
    vexo.shutdown

    assert_equal [%w[a]], @api.requests.each_index.map { |i| events(i) }
    assert_empty @errors
  end

  def test_counts_a_2xx_body_that_is_not_json_as_a_success
    vexo = client([[200, {}, "<html>ok</html>"]])
    vexo.capture(distinct_id: "u", event: "a")
    vexo.flush

    assert_equal 1, @api.requests.size
    assert_empty @errors
  end

  def test_sends_what_is_queued_at_exit
    client
    output = ruby(<<~RUBY, @api.host)
      require "vexo"
      vexo = Vexo::Client.new("test-key", host: ARGV[0], flush_interval: 60)
      vexo.capture(distinct_id: "u", event: "at_exit")
    RUBY

    assert_equal "", output
    assert_equal %w[at_exit], events(0)
  end

  def test_exit_never_waits_past_shutdown_timeout
    client([:hang] * 3)
    started = monotonic
    output = ruby(<<~RUBY, @api.host)
      require "vexo"
      vexo = Vexo::Client.new("test-key", host: ARGV[0], flush_at: 1, timeout: 30, shutdown_timeout: 1)
      3.times { |i| vexo.capture(distinct_id: "u", event: "e\#{i}") }
    RUBY

    assert_operator monotonic - started, :<, 3
    assert_match(/shutdown deadline reached/, output)
  end

  def test_concurrent_callers_send_every_message_once
    vexo = client(flush_at: 50)
    Array.new(8) { |t| Thread.new { 250.times { |i| vexo.capture(distinct_id: "t#{t}", event: "e#{i}") } } }.each(&:join)
    vexo.flush

    ids = @api.requests.flat_map { |request| request[:body]["batch"].map { |message| message["messageId"] } }
    assert_equal 2000, ids.size
    assert_equal 2000, ids.uniq.size
    assert_empty @errors
  end

  def test_a_forked_child_drops_the_parents_queue_and_sends_from_its_own_thread
    skip "needs fork" unless Process.respond_to?(:fork)

    vexo = client(flush_at: 2)
    vexo.capture(distinct_id: "parent", event: "before_fork")
    pid = fork do
      # Reaches flush_at only if the child's queue starts empty; then the child's own thread sends it.
      vexo.capture(distinct_id: "child", event: "c1")
      vexo.capture(distinct_id: "child", event: "c2")
      sleep(0.5)
      exit!(0)
    end
    Process.wait(pid)

    wait_for_requests(1)
    assert_equal %w[c1 c2], events(0)
    vexo.flush
    assert_equal %w[before_fork], events(1)
  end
end

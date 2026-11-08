# frozen_string_literal: true

require "json"
require "net/http"
require "securerandom"
require_relative "vexo/version"

# Send server-side events and identifies to Vexo. Standard library only.
module Vexo
  # What the client reports to +on_error+; it never raises one from +capture+ or +identify+.
  #
  # +batch+ holds the messages of a request that failed, +status+ the HTTP status of a rejected request, and +dropped+
  # the messages the API didn't store, as <tt>{ message:, reason: }</tt> hashes.
  class Error < StandardError
    attr_reader :status, :dropped
    attr_accessor :batch

    def initialize(message = nil, status: nil, batch: nil, dropped: nil)
      super(message)
      @status = status
      @batch = batch
      @dropped = dropped
    end
  end

  # Queues messages and sends them in batches from a background thread. Safe to share between threads, and to create
  # before a fork: a forked child starts with an empty queue and its own thread.
  class Client
    DEFAULT_HOST = "https://api.vexo.co"
    # The API rejects a batch of more than 500 messages or a body over 5 MB.
    MAX_BATCH_SIZE = 500
    MAX_BODY_BYTES = 5 * 1024 * 1024
    BATCH_ENVELOPE_BYTES = '{"batch":[]}'.bytesize
    MAX_BACKOFF_SECONDS = 30.0
    MAX_RETRY_AFTER_SECONDS = 60.0
    # The most shutdown waits, past the deadline, for the aborted background thread to report its batch (Ruby checks
    # for the interrupt on a 100 ms timer).
    ABORT_GRACE_SECONDS = 0.5
    # Raised in the background thread at the shutdown deadline to end its request or backoff wait.
    Abort = Class.new(StandardError)
    private_constant :DEFAULT_HOST, :MAX_BATCH_SIZE, :MAX_BODY_BYTES, :BATCH_ENVELOPE_BYTES, :MAX_BACKOFF_SECONDS,
                     :MAX_RETRY_AFTER_SECONDS, :ABORT_GRACE_SECONDS, :Abort

    def initialize(api_key, host: DEFAULT_HOST, flush_at: 100, flush_interval: 10.0, max_retries: 3,
                   max_queue_size: 10_000, timeout: 10.0, shutdown_timeout: 10.0, on_error: nil)
      raise ArgumentError, "vexo: an API key is required" if blank?(api_key)

      @uri = URI(host.to_s.sub(%r{/+\z}, ""))
      @headers = { "Content-Type" => "application/json", "X-Api-Key" => api_key.to_s,
                   "User-Agent" => "vexo-ruby/#{VERSION}" }
      @flush_at = flush_at.clamp(1, MAX_BATCH_SIZE)
      @flush_interval = flush_interval
      @max_retries = max_retries
      @max_queue_size = max_queue_size
      @timeout = timeout
      @shutdown_timeout = shutdown_timeout
      @on_error = on_error || ->(error) { warn(error.message) }

      @mutex = Mutex.new
      @closed = false
      # Set by a batch that used up its retries: later batches get one attempt until a request succeeds.
      @circuit_open = false
      @deadline = nil
      reset_state
      at_exit { shutdown }
    end

    # Queues an event. +timestamp+ (a Time, Date, epoch milliseconds, or an ISO 8601 String with an offset) defaults to
    # now; +message_id+ defaults to a random UUID.
    def capture(distinct_id: nil, event: nil, properties: nil, timestamp: nil, message_id: nil)
      return report(Error.new("vexo: capture needs a distinct_id and an event")) if blank?(distinct_id) || blank?(event)

      enqueue(message_id) do
        { "type" => "capture", "distinctId" => distinct_id.to_s, "event" => event.to_s,
          "properties" => properties || {}, "timestamp" => epoch_millis(timestamp) }
      end
    end

    # Queues an identify: merges +traits+ into the person with this distinct id. A trait set to nil is removed.
    def identify(distinct_id: nil, traits: nil)
      return report(Error.new("vexo: identify needs a distinct_id")) if blank?(distinct_id)

      enqueue(nil) { { "type" => "identify", "distinctId" => distinct_id.to_s, "traits" => traits || {} } }
    end

    # Sends everything queued, blocking until the queue is empty. Send failures go to +on_error+.
    def flush
      @mutex.synchronize { reset_if_forked }
      @send_lock.synchronize { drain }
      nil
    end

    # Stops the background thread and sends everything queued, giving up after +shutdown_timeout+ seconds: the request
    # or backoff wait in progress then is aborted, and what wasn't sent is reported to +on_error+. Runs at exit too, and
    # is safe to call again.
    def shutdown
      @mutex.synchronize do
        reset_if_forked
        @deadline ||= monotonic + @shutdown_timeout
        @closed = true
        @wake.broadcast
      end
      stop_worker
      return drop_queue unless lock_until_deadline

      begin
        drain
      ensure
        @send_lock.unlock
      end
      nil
    end

    private

    # Also runs in a forked child (Puma, Unicorn, Passenger, Sidekiq swarm), which starts with an empty queue and its own
    # thread; the parent keeps sending what it queued before the fork.
    def reset_state
      @pid = Process.pid
      @queue = []
      @worker = nil
      @send_lock = Mutex.new
      @wake = ConditionVariable.new
    end

    # Every public method calls this first (holding @mutex), so a child never sends its copy of the parent's queue.
    def reset_if_forked
      reset_state unless @pid == Process.pid
    end

    def enqueue(message_id)
      return report(Error.new("vexo: the client is shut down; message dropped")) if @closed

      begin
        message = yield
        message["messageId"] = blank?(message_id) ? SecureRandom.uuid : message_id.to_s
        json = JSON.generate(message)
      rescue StandardError => e
        # Only this message is dropped: a NaN, a circular reference, invalid UTF-8, or a timestamp that isn't a time.
        return report(Error.new("vexo: message dropped: #{e.message}"))
      end
      return report(Error.new("vexo: message dropped, it is larger than 5 MB")) if
        json.bytesize + BATCH_ENVELOPE_BYTES > MAX_BODY_BYTES

      full = @mutex.synchronize do
        reset_if_forked
        next true if @queue.size >= @max_queue_size

        @queue << [message, json]
        start_worker
        @wake.signal if @queue.size >= @flush_at
        false
      end
      full ? report(Error.new("vexo: the queue is full; message dropped")) : nil
    end

    # Holding @mutex. Started by the first message, so a client created before a fork (Puma's preload_app!) runs no
    # thread in the parent unless the parent queues something.
    def start_worker
      return if @closed || @worker&.alive?

      @worker = Thread.new { run }
      @worker.name = "vexo-flush"
    end

    def run
      # Abort lands only in a request or a backoff wait (see #abortable), never while the thread holds a lock.
      Thread.handle_interrupt(Abort => :never) do
        until @closed
          @mutex.synchronize { @wake.wait(@mutex, @flush_interval) unless @closed || @queue.size >= @flush_at }
          @send_lock.synchronize { drain } unless @closed
        end
      end
    rescue Abort
      nil
    end

    # Waits for the background thread until the deadline, then aborts what it's doing so that it reports its batch.
    def stop_worker
      worker = @worker
      return if worker.nil? || worker == Thread.current || worker.join(remaining)

      worker.raise(Abort, "shutdown deadline reached")
      worker.join(ABORT_GRACE_SECONDS)
    end

    def abortable(&block)
      Thread.handle_interrupt(Abort => :immediate, &block)
    end

    # Holding @send_lock.
    def drain
      loop do
        return drop_queue if remaining <= 0

        batch = next_batch
        return if batch.empty?

        begin
          deliver(batch)
        rescue StandardError => e # every send failure is reported, never raised
          error = e.is_a?(Error) ? e : Error.new("vexo: batch failed: #{e.message}")
          error.batch = batch.map(&:first)
          report(error)
        end
      end
    end

    # Up to flush_at messages that fit in one request body.
    def next_batch
      @mutex.synchronize do
        bytes = BATCH_ENVELOPE_BYTES
        count = 0
        while count < @queue.size && count < @flush_at && bytes + @queue[count][1].bytesize + count <= MAX_BODY_BYTES
          bytes += @queue[count][1].bytesize
          count += 1
        end
        @queue.shift(count)
      end
    end

    def drop_queue
      dropped = @mutex.synchronize { @queue.shift(@queue.size) }
      return if dropped.empty?

      report(Error.new("vexo: shutdown deadline reached; #{dropped.size} message(s) dropped",
                       batch: dropped.map(&:first)))
    end

    # POST /v1/batch. Network errors, 429, and 5xx are retried with exponential backoff (or the API's Retry-After);
    # messages the API reports as failed are sent again the same way. Dropped messages are reported, not retried.
    def deliver(batch)
      max_retries = @circuit_open ? 0 : @max_retries
      attempt = 0
      loop do
        begin
          response = post(batch)
        rescue StandardError => e
          if attempt >= max_retries || remaining <= 0
            @circuit_open = true
            raise Error, "vexo: batch failed: #{e.message}"
          end
          pause(backoff(attempt))
          attempt += 1
          next
        end

        status = response.code.to_i
        if status.between?(200, 299)
          @circuit_open = false
          result = results(response.body)
          report_results(batch, result["dropped"], "the server dropped")
          failed = (result["failed"] || []).filter_map { |f| entry(batch, f["index"]) }
          return if failed.empty?
          return report_results(batch, result["failed"], "the server could not store") if
            attempt >= max_retries || remaining <= 0

          batch = failed
          pause(backoff(attempt))
        else
          retryable = status == 429 || status >= 500
          if !retryable || attempt >= max_retries || remaining <= 0
            @circuit_open = true if retryable
            raise Error.new("vexo: batch rejected with HTTP #{status}: #{response.body}", status: status)
          end
          pause(retry_after(response["Retry-After"]) || backoff(attempt))
        end
        attempt += 1
      end
    end

    def post(batch)
      body = "{\"batch\":[#{batch.map(&:last).join(",")}]}"
      timeout = [[@timeout, remaining].min, 0.001].max
      abortable do
        Net::HTTP.start(@uri.host, @uri.port, use_ssl: @uri.scheme == "https", open_timeout: timeout,
                        read_timeout: timeout, write_timeout: timeout) do |http|
          http.post("#{@uri.path}/v1/batch", body, @headers)
        end
      end
    end

    # The data of the API's envelope. A 2xx body that isn't one counts as a success with nothing dropped or failed.
    def results(body)
      data = JSON.parse(body.to_s)["data"]
      data.is_a?(Hash) ? data : {}
    rescue StandardError
      {}
    end

    def report_results(batch, results, what)
      return if results.nil? || results.empty?

      dropped = results.map { |r| { message: entry(batch, r["index"])&.first, reason: r["reason"] } }
      report(Error.new("vexo: #{what} #{results.size} message(s): #{results[0]["reason"]}", dropped: dropped))
    end

    def entry(batch, index)
      batch[index] if index.is_a?(Integer) && index >= 0
    end

    def report(error)
      @on_error.call(error)
      nil
    rescue StandardError => e
      warn("vexo: on_error raised #{e.class}: #{e.message}")
    end

    # Polls, because a Mutex has no timed lock.
    def lock_until_deadline
      until @send_lock.try_lock
        return false if remaining <= 0

        sleep(0.01)
      end
      true
    end

    # Seconds until the shutdown deadline; infinite before shutdown.
    def remaining
      @deadline ? [@deadline - monotonic, 0.0].max : Float::INFINITY
    end

    def pause(seconds)
      abortable { sleep([seconds, remaining].min) }
    end

    def backoff(attempt)
      base = [2.0**attempt, MAX_BACKOFF_SECONDS].min
      (base / 2) + (rand * base / 2)
    end

    def retry_after(header)
      Float(header, exception: false)&.clamp(0.0, MAX_RETRY_AFTER_SECONDS)
    end

    # Epoch milliseconds as an Integer; a String (ISO 8601 with an offset) is sent as is.
    def epoch_millis(timestamp)
      case timestamp
      when nil then Process.clock_gettime(Process::CLOCK_REALTIME, :millisecond)
      when Numeric then timestamp.to_i
      when String then timestamp
      else
        raise ArgumentError, "timestamp must be a Time, a Date, epoch milliseconds, or an ISO 8601 String" unless
          timestamp.respond_to?(:to_time)

        # A Date is midnight UTC; a Time, DateTime, or ActiveSupport::TimeWithZone carries its own offset.
        time = timestamp.respond_to?(:hour) ? timestamp.to_time : Time.utc(timestamp.year, timestamp.month, timestamp.day)
        (time.to_r * 1000).to_i
      end
    end

    def monotonic
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end

    def blank?(value)
      value.nil? || value.to_s.strip.empty?
    end
  end
end

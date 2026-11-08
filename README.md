# vexo-ruby

Send events and identifies to [Vexo](https://vexo.co) from your Ruby backend: purchases, subscription changes, background jobs, anything your app's SDK can't see. Server events land in the same app as your mobile and web events, bill as events, and join the person who has the same distinct ID.

- Zero runtime dependencies (Ruby 3.0 or later; `net/http` and `json` from the standard library).
- Batches messages on a background thread, flushes on size and on an interval, and flushes at exit within a deadline.
- Retries network errors, `429`, and `5xx` responses (and the messages the API reports as `failed`) with exponential backoff, and honors `Retry-After`.
- Keeps each request under the API's 5 MB body limit, and is safe to create before a fork (Puma, Unicorn, Passenger, Sidekiq Enterprise swarms).

## Install

Add the gem to your `Gemfile`:

```ruby
gem "vexo", "~> 0.1.0"
```

Or install it directly:

```bash
gem install vexo
```

## Usage

```ruby
require "vexo"

vexo = Vexo::Client.new(ENV.fetch("VEXO_API_KEY"))

# Attach traits to the person. Use the same ID that you pass to identifyDevice() in the app SDK.
vexo.identify(distinct_id: "user@example.com", traits: { plan: "pro", company: "Acme" })

# Record an event.
vexo.capture(distinct_id: "user@example.com", event: "purchase_completed",
             properties: { amount: 49.9, currency: "USD" })

# Optional: send everything still queued now. It also runs at exit.
vexo.shutdown
```

`capture` and `identify` only queue the message, so they never block a request. Create one client for each process and share it: it's safe to call from many threads.

Copy your app's API key from the app's [settings](https://docs.vexo.co/settings/app-settings/app-general-information) in the Vexo dashboard. It's the same key your app SDK uses, so it isn't a secret; see [Security](https://docs.vexo.co/server-side/capture#security).

## Shutdown

The client registers `shutdown` with `at_exit`, so a process that exits normally sends what it queued. `shutdown` keeps sending for up to `shutdown_timeout` seconds. At that deadline it aborts the request or retry wait in progress and reports every message it couldn't send to `on_error`, so it never holds your process past the deadline.

A process that ends without running `at_exit` hooks (for example, on `SIGKILL` or `exit!`) loses the messages still queued. If your server stops workers that way, call `shutdown` from its worker-shutdown hook.

## Rails

Create the client in an initializer:

```ruby
# config/initializers/vexo.rb
VEXO = Vexo::Client.new(
  ENV.fetch("VEXO_API_KEY"),
  on_error: ->(error) { Rails.logger.warn(error.message) }
)
```

Then call it from controllers, models, and jobs:

```ruby
VEXO.capture(distinct_id: current_user.email, event: "plan_upgraded", properties: { from: "free", to: "pro" })
```

### Puma, Unicorn, and Passenger

You can create the client before the server forks its workers, for example with Puma's `preload_app!`. The client notices the fork the first time a worker uses it: the worker starts with an empty queue and its own background thread, and the parent process keeps sending only what it queued itself. You don't need an `on_worker_boot` hook.

### Sidekiq and Active Job

Call the client from a job like from any other code:

```ruby
class RenewSubscriptionJob < ApplicationJob
  def perform(subscription_id)
    subscription = Subscription.find(subscription_id)
    subscription.renew!
    VEXO.capture(
      distinct_id: subscription.user.email,
      event: "subscription_renewed",
      properties: { plan: subscription.plan, amount: subscription.price },
      # A retried job sends the same message ID, so the event is stored once.
      message_id: "renewal-#{subscription.id}-#{subscription.current_period_start.to_i}"
    )
  end
end
```

Sidekiq runs `at_exit` hooks when it shuts down normally, so the queue is sent within `shutdown_timeout` (10 seconds by default, inside Sidekiq's default 25-second shutdown timeout). Sidekiq Enterprise swarms fork their processes, which the client handles as described in [Puma, Unicorn, and Passenger](#puma-unicorn-and-passenger).

The API stores each message ID once within 5 days. Pass your own `message_id` when the same work can run twice, as in a retried job, and let the client generate one otherwise.

## Serverless

On AWS Lambda and similar platforms, the runtime freezes your process between invocations and can end it without running `at_exit` hooks. Create the client outside your handler, and call `flush` before the handler returns:

```ruby
VEXO = Vexo::Client.new(ENV.fetch("VEXO_API_KEY"))

def handler(event:, context:)
  VEXO.capture(distinct_id: event["userId"], event: "export_finished")
  { statusCode: 200 }
ensure
  VEXO.flush
end
```

## API

### `Vexo::Client.new(api_key, **options)`

Raises `ArgumentError` when `api_key` is missing.

| Option | Default | Description |
| --- | --- | --- |
| `host` | `"https://api.vexo.co"` | API base URL. |
| `flush_at` | `100` | Send a batch when this many messages are queued (1 to 500). |
| `flush_interval` | `10.0` | Send whatever is queued every this many seconds. `0` turns the timer off, so messages go out only on `flush_at`, `flush`, and `shutdown`. |
| `max_retries` | `3` | Retries for network errors, `429`, and `5xx` responses. |
| `max_queue_size` | `10_000` | Messages kept in memory; new messages are dropped once it's full. |
| `timeout` | `10.0` | Request timeout in seconds, for each of connecting, writing, and reading. |
| `shutdown_timeout` | `10.0` | How long `shutdown` keeps sending, in seconds. |
| `on_error` | `warn` the message | Called with a `Vexo::Error` for each send failure and for messages dropped by the client or the server. |

### `capture(distinct_id:, event:, properties: nil, timestamp: nil, message_id: nil)`

Queues an event and returns `nil`. `timestamp` is a `Time`, a `Date` (read as midnight UTC), a `DateTime`, an `ActiveSupport::TimeWithZone`, epoch milliseconds, or an ISO 8601 string with an offset, and defaults to the time you call `capture`. Everything except a string is sent as integer epoch milliseconds. `message_id` defaults to a random UUID.

### `identify(distinct_id:, traits: nil)`

Queues an identify and returns `nil`. Traits merge into the person's existing traits; a trait set to `nil` is removed.

### `flush`

Sends everything queued, blocking until the queue is empty.

### `shutdown`

Stops the background thread and sends everything queued, for up to `shutdown_timeout` seconds. It's safe to call more than once.

## Errors and dropped messages

The client never raises from `capture` or `identify`. Failures go to `on_error` as a `Vexo::Error`:

- A message without a `distinct_id` (or a capture without an `event`), or one queued after `shutdown`, is dropped and reported.
- Values that JSON can't encode natively are sent as strings, in the form every Vexo SDK uses: a `Time`, `DateTime`, or `ActiveSupport::TimeWithZone` as an ISO 8601 string in UTC, such as `"2026-11-07T12:00:00.000Z"`; a `Date` as `"2026-11-07"`; a `BigDecimal` in plain decimal notation with all its digits (`"49.9"`, never `"0.499e2"`); and a `Symbol` as its name. Invalid UTF-8 in a string, or in the bytes of a binary string, is replaced with `U+FFFD`, and the message is sent.
- A message that can't be serialized (a `NaN` or infinite number, including a `BigDecimal` one, a hash or array that contains itself, or a `timestamp` of an unsupported type) or that is over 5 MB is dropped when you queue it; the rest of the queue is unaffected.
- A batch that still fails after the retries, or that the API rejects with a `4xx`, arrives with `batch` (the messages it held) and `status` (the HTTP status, when there was a response). The original exception of a network error is the error's `cause`. After that, the next batches get a single attempt until a request succeeds, so an unreachable API doesn't hold up your process.
- A message the API dropped (for example, properties over 64 KB) arrives with `dropped`, a list of `{ message:, reason: }` hashes. Messages the API reports as `failed` are retried, and reported the same way if they still fail.
- Messages still queued at the shutdown deadline arrive as one error with `batch`.

Every message carries a `messageId`, so a retried batch is stored once.

## Development

Run the tests on your Ruby, and on the oldest supported one in Docker:

```bash
ruby -Ilib test/vexo_test.rb
docker run --rm -v "$PWD":/src:ro -w /src ruby:3.0-slim ruby -Ilib test/vexo_test.rb
```

Run the shared conformance scenarios from a checkout of `vexo-sdk-conformance`:

```bash
python3 ../vexo-sdk-conformance/run.py --driver "ruby conformance/driver.rb"
```

## License

MIT

# Sidekiq CloudWatch Metrics

Runs a thread inside your Sidekiq processes to report metrics to CloudWatch
useful for autoscaling and keeping an eye on your queues.

Optimised for Sidekiq Enterprise with leader election, but works everywhere!

<img width="1055" alt="Screenshot of Sidekiq metrics in a CloudWatch dashboard" src="https://user-images.githubusercontent.com/14028/44190767-9fd66280-a16b-11e8-8b12-3d5e0641c15f.png">

## Installation

Add this gem to your application’s Gemfile near sidekiq and then run `bundle install`:

```ruby
gem "sidekiq"
gem "sidekiq-cloudwatchmetrics"
```

## Usage

Add near your Sidekiq configuration, like in `config/initializers/sidekiq.rb` in Rails:

```ruby
require "sidekiq"
require "sidekiq/cloudwatchmetrics"

Sidekiq::CloudWatchMetrics.enable!
```

By default this assumes you're running on an EC2 instance with an instance role
that can publish CloudWatch metrics, or that you've supplied AWS credentials
through environment variables that aws-sdk expects. You can also explicitly
supply an [aws-sdk CloudWatch Client instance][cwclient]:

```ruby
Sidekiq::CloudWatchMetrics.enable!(client: Aws::CloudWatch::Client.new)
```

  [cwclient]: https://docs.aws.amazon.com/sdk-for-ruby/v3/api/Aws/CloudWatch/Client.html

The default namespace for metrics is "Sidekiq". You can configure this with the `namespace` option:

```ruby
Sidekiq::CloudWatchMetrics.enable!(namespace: "Sidekiq-Staging")
```

Metrics are published every 60 seconds by default. You can adjust this with the `interval` option:

```ruby
Sidekiq::CloudWatchMetrics.enable!(interval: 30)
```

When the interval is less than 60 seconds the metrics are published as
[high-resolution metrics][highres] (1-second storage resolution), suitable for
fast-reacting alarms and burst auto-scaling.

  [highres]: https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/publishingMetrics.html#high-resolution-metrics

### Choosing which metrics to publish

By default every metric is published. To opt in to a subset, pass a `metrics:`
array of symbols:

```ruby
# Just publish queue depth + latency, useful for autoscaling alarms
Sidekiq::CloudWatchMetrics.enable!(
  interval: 10,
  metrics: %i[queue_size queue_latency],
)
```

The publisher skips the upstream Sidekiq calls it doesn't need — e.g. if you
only ask for queue metrics it won't enumerate the process set, and if you only
ask for global stats it won't iterate queues.

`enable!` may be called multiple times to register additional publishers with
different cadences and filters. For example, a slow full-fat publisher plus a
fast queue-only publisher for burst scaling:

```ruby
Sidekiq::CloudWatchMetrics.enable!  # everything, every 60s
Sidekiq::CloudWatchMetrics.enable!(
  interval: 10,
  metrics: %i[queue_size queue_latency],
)
```

Available metric keys:

| Symbol                  | CloudWatch metric     | Scope                                  |
| ----------------------- | --------------------- | -------------------------------------- |
| `:processed_jobs`       | `ProcessedJobs`       | global                                 |
| `:failed_jobs`          | `FailedJobs`          | global                                 |
| `:enqueued_jobs`        | `EnqueuedJobs`        | global                                 |
| `:scheduled_jobs`       | `ScheduledJobs`       | global                                 |
| `:retry_jobs`           | `RetryJobs`           | global                                 |
| `:dead_jobs`            | `DeadJobs`            | global                                 |
| `:workers`              | `Workers`             | global                                 |
| `:processes`            | `Processes`           | global                                 |
| `:default_queue_latency`| `DefaultQueueLatency` | global                                 |
| `:capacity`             | `Capacity`            | global aggregate over all processes    |
| `:utilization`          | `Utilization`         | global aggregate over all processes    |
| `:tag_capacity`         | `Capacity`            | per process tag                        |
| `:tag_utilization`      | `Utilization`         | per process tag                        |
| `:process_utilization`  | `Utilization`         | per process (Hostname dimension)       |
| `:queue_size`           | `QueueSize`           | per queue                              |
| `:queue_latency`        | `QueueLatency`        | per queue                              |
| `:job_execution_time_p50` | `JobExecutionTimeP50` | per job class (Sidekiq 7+)           |
| `:job_execution_time_p95` | `JobExecutionTimeP95` | per job class (Sidekiq 7+)           |
| `:job_execution_time_p99` | `JobExecutionTimeP99` | per job class (Sidekiq 7+)           |

Unknown symbols raise `ArgumentError` at boot.

The `job_execution_time_*` metrics are derived from the execution histograms
Sidekiq 7+ records in Redis via its built-in `ExecutionTracker` middleware.
Each tick reads the previous full minute's histograms once and computes the
requested percentiles from the bucket counts (resolution is whichever
`Sidekiq::Metrics::Histogram::BUCKET_INTERVALS` bucket the percentile falls
into — e.g. `1.7s`, `2.5s`, `3.8s`). They are silently skipped on Sidekiq
versions that don't ship `Sidekiq::Metrics`. Use them on the standard 60s
publisher, not on burst publishers — the source data only updates once per
minute.

The legacy `process_metrics:` boolean is still accepted for backwards
compatibility but emits a deprecation warning — prefer the `metrics:` option
(omit `:process_utilization` to disable per-process utilization).

### Leader election (single-publisher across N nodes)

On Sidekiq Enterprise the publisher already runs on a single leader process
(via `config.on(:leader)`). On OSS Sidekiq the default is that every node
publishes — CloudWatch dedupes datapoints to the same metric per second so
your dashboards stay correct, but you pay N× the `put_metric_data` API
calls and the `SampleCount` of each datapoint becomes N instead of 1.

To elect a single publisher across all your OSS Sidekiq nodes, opt in to
the Redis-backed leader election:

```ruby
Sidekiq::CloudWatchMetrics.enable!(
  leader_election: :redis,
)
```

How it works: every node still starts the publisher thread, but inside
each tick it tries to acquire (or extend) a Redis `SET key value NX EX`
lock. Only the lock holder actually calls `put_metric_data`. The lock key
is scoped by `namespace:` so multiple deployments sharing a Redis don't
fight over the same lock. TTL defaults to 3× your publish interval. On
clean shutdown the lock is released so failover is instant; on a crash
the lock expires naturally and any other node picks up at its next tick.

Use this **only** on the standard cadence publisher. Don't enable it on a
fast burst publisher (e.g. 10s for `queue_size`/`queue_latency` driving
auto-scaling) — there you want every node to keep refreshing the values
even if the leader hangs.

```ruby
# Standard publisher: single leader, low API churn
Sidekiq::CloudWatchMetrics.enable!(leader_election: :redis)

# Burst publisher: every node, no leader, fast failover
Sidekiq::CloudWatchMetrics.enable!(
  interval: 10,
  metrics: %i[queue_size queue_latency],
)
```

### Capping `JobClass` cardinality

`job_execution_time_p*` metrics publish one CloudWatch series per
distinct Sidekiq job class, and every series is a separate billable
custom metric. Long-tail apps with hundreds of job classes can pay for
a lot of noise that rarely informs an operator decision.

By default the publisher caps `JobClass` cardinality at **20**: the 20
classes with the most samples in the last minute keep their own
series, and everything else is summed bucket-by-bucket into a single
`JobClass=(other)` histogram. The parentheses make the rollup label
distinct from any real Ruby class name. Operators still see overall
long-tail latency without paying per-class for the long tail.

```ruby
Sidekiq::CloudWatchMetrics.enable!(
  metrics: %i[job_execution_time_p50 job_execution_time_p99],
  max_job_classes: 20,  # default
)
```

Pass `max_job_classes: nil` to publish every class with activity (the
previous behaviour) or any positive integer to tune the cap.

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake spec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/sj26/sidekiq-cloudwatchmetrics.

## License

The gem is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).


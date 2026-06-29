# frozen_string_literal: true

require "securerandom"
require "socket"

require "sidekiq"
require "sidekiq/api"

require "aws-sdk-cloudwatch"

module Sidekiq::CloudWatchMetrics
  # Cluster-wide leader election backed by a Redis SETNX-with-TTL lock, for
  # OSS Sidekiq deployments that want a single publisher across N nodes (the
  # default behaviour is to publish from every node, which CloudWatch handles
  # fine but costs N× the put_metric_data API calls).
  #
  # Election is re-checked on every publish tick. If the holder dies, any
  # other node picks up the lock at the next tick — failover takes at most
  # one interval. On clean shutdown the lock is released so failover is
  # instantaneous.
  class RedisLeader
    DEFAULT_TTL_MULTIPLIER = 3

    def initialize(key:, interval:, ttl: nil, id: nil)
      @key = key
      @ttl = ttl || (interval * DEFAULT_TTL_MULTIPLIER)
      @id = id || "#{Socket.gethostname}-#{Process.pid}-#{SecureRandom.hex(4)}"
    end

    attr_reader :id, :key, :ttl

    # Returns true if this process now holds the lock (either freshly
    # acquired or extended). The set..ex..nx command is atomic in Redis 2.6+;
    # the read-then-extend on the else branch races at most by one TTL, which
    # for our purpose is harmless (publishes are idempotent).
    def acquire_or_extend
      Sidekiq.redis do |conn|
        return true if conn.set(@key, @id, nx: true, ex: @ttl)

        current = conn.get(@key)
        if current == @id
          conn.expire(@key, @ttl)
          true
        else
          false
        end
      end
    end

    # Best-effort release on shutdown. If the lock has already expired and
    # someone else holds it, we leave it alone.
    def release
      Sidekiq.redis do |conn|
        conn.del(@key) if conn.get(@key) == @id
      end
    rescue => e
      Sidekiq.logger.debug { "RedisLeader release failed: #{e}" } if Sidekiq.respond_to?(:logger)
    end
  end

  # Records and reads per-job-class execution durations in short-lived Redis
  # lists, keyed by (namespace, UTC minute, job class). The Publisher drains a
  # completed minute and computes true percentile latencies from the raw
  # samples — unlike Sidekiq's own execution histogram, which buckets durations
  # and collapses everything slower than ~335s into a single "Slow" bucket.
  class ExecutionSamples
    # Sample lists and the per-minute class index expire this long after their
    # last write — a backstop in case the leader misses a drain tick. Safely
    # longer than the 60s publish cadence.
    TTL_SECONDS = 180

    # Most samples retained per class per minute. Percentiles stay accurate far
    # below this; the cap only bounds Redis memory for pathological bursts. We
    # keep the most recent samples (LTRIM to the tail).
    DEFAULT_SAMPLE_CAP = 5_000

    def initialize(namespace:, sample_cap: DEFAULT_SAMPLE_CAP)
      @prefix = "sidekiq-cloudwatchmetrics:exectimes:#{namespace}"
      @sample_cap = sample_cap
    end

    # Append one execution time (milliseconds) to the current minute's list for
    # `klass` and register the class in that minute's index. Best-effort: any
    # Redis error is swallowed so recording never disturbs the job being run.
    def record(klass, elapsed_ms, now = Time.now)
      window = window_key(now)
      list = list_key(window, klass)
      index = index_key(window)

      Sidekiq.redis do |conn|
        conn.rpush(list, elapsed_ms.round(3))
        conn.ltrim(list, -@sample_cap, -1)
        conn.expire(list, TTL_SECONDS)
        conn.sadd(index, klass)
        conn.expire(index, TTL_SECONDS)
      end
      nil
    rescue => e
      Sidekiq.logger.debug { "[sidekiq-cloudwatchmetrics] failed to record execution time: #{e}" } if Sidekiq.respond_to?(:logger)
      nil
    end

    # Returns { klass => [ms, ...] } for the minute containing `time`, deleting
    # the drained keys so a re-run (or a second leader after failover) sees
    # nothing rather than double-publishing.
    def drain(time)
      window = window_key(time)
      index = index_key(window)
      samples = {}

      Sidekiq.redis do |conn|
        classes = conn.smembers(index)
        return {} if classes.nil? || classes.empty?

        classes.each do |klass|
          list = list_key(window, klass)
          values = conn.lrange(list, 0, -1)
          conn.del(list)
          next if values.nil? || values.empty?
          samples[klass] = values.map(&:to_f)
        end
        conn.del(index)
      end

      samples
    end

    private def window_key(time)
      time.utc.strftime("%Y%m%dT%H%M")
    end

    private def list_key(window, klass)
      "#{@prefix}:#{window}:samples:#{klass}"
    end

    private def index_key(window)
      "#{@prefix}:#{window}:classes"
    end
  end

  # Sidekiq server middleware that records how long each job ran, so the
  # Publisher can report true per-class percentile latencies. Only executions
  # at or above `min_job_seconds` are stored — the same threshold the Publisher
  # uses to decide which classes are worth a billable CloudWatch metric — so
  # fast jobs cost nothing beyond a monotonic clock read. A nil threshold
  # records every execution.
  class ExecutionRecorder
    def initialize(namespace, min_job_seconds, sample_cap = ExecutionSamples::DEFAULT_SAMPLE_CAP)
      @samples = ExecutionSamples.new(namespace: namespace, sample_cap: sample_cap)
      @min_job_ms = min_job_seconds.nil? ? 0.0 : Float(min_job_seconds) * 1000.0
    end

    def call(_worker, job, _queue)
      started = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
      yield
    ensure
      elapsed_ms = (::Process.clock_gettime(::Process::CLOCK_MONOTONIC) - started) * 1000.0
      if elapsed_ms >= @min_job_ms
        klass = job["wrapped"] || job["class"]
        @samples.record(klass, elapsed_ms) if klass
      end
    end
  end

  def self.enable!(**kwargs)
    Sidekiq.configure_server do |config|
      publisher = Publisher.new(config: config, **kwargs)

      # Per-job-class execution-time percentiles are computed from real
      # durations we record ourselves (see ExecutionRecorder), so install the
      # recording middleware whenever this publisher owns those metrics. The
      # guard keeps it a no-op when enable! is called more than once — e.g. a
      # 60s metrics publisher plus a fast queue-only publisher.
      if publisher.records_execution_times?
        config.server_middleware do |chain|
          unless chain.exists?(ExecutionRecorder)
            chain.add(ExecutionRecorder, publisher.namespace, publisher.min_job_seconds, publisher.execution_sample_cap)
          end
        end
      end

      # Sidekiq enterprise has a globally unique leader thread, making it
      # easier to publish the cluster-wide metrics from one place.
      if defined?(Sidekiq::Enterprise)
        config.on(:leader) do
          publisher.start
        end
      else
        # Otherwise pubishing from every node doesn't hurt, it's just wasteful
        config.on(:startup) do
          publisher.start
        end
      end

      config.on(:quiet) do
        publisher.quiet if publisher.running?
      end

      config.on(:shutdown) do
        publisher.stop if publisher.running?
      end
    end
  end

  class Publisher
    begin
      require "sidekiq/util"
      include Sidekiq::Util
    rescue LoadError
      # Sidekiq 6.5 refactored to use Sidekiq::Component
      require "sidekiq/component"
      include Sidekiq::Component
    end

    DEFAULT_INTERVAL = 60 # seconds

    # Metric keys grouped by the upstream Sidekiq data they need, so the
    # publisher can skip work it doesn't have to do on each tick.
    GLOBAL_STATS_METRICS = %i[
      processed_jobs
      failed_jobs
      enqueued_jobs
      scheduled_jobs
      retry_jobs
      dead_jobs
      workers
      processes
      default_queue_latency
    ].freeze

    GLOBAL_AGGREGATE_METRICS = %i[capacity utilization].freeze
    TAG_METRICS = %i[tag_capacity tag_utilization].freeze
    PROCESS_METRICS = %i[process_utilization].freeze
    QUEUE_METRICS = %i[queue_size queue_latency].freeze

    # Per-job-class execution time percentiles, computed from the real job
    # durations ExecutionRecorder records into Redis. Reporting true seconds
    # (no histogram bucketing) means slow jobs are no longer clamped to ~335s.
    EXECUTION_TIME_METRICS = %i[
      job_execution_time_p50
      job_execution_time_p95
      job_execution_time_p99
    ].freeze

    EXECUTION_TIME_PERCENTILES = {
      job_execution_time_p50: [0.50, "JobExecutionTimeP50"],
      job_execution_time_p95: [0.95, "JobExecutionTimeP95"],
      job_execution_time_p99: [0.99, "JobExecutionTimeP99"],
    }.freeze

    # Drain the minute that finished one window ago, so we never read a window
    # that jobs are still completing into.
    EXECUTION_WINDOW_SECONDS = 60

    # Each distinct JobClass dimension value is a separate CloudWatch billable
    # metric; long-tail apps with hundreds of job classes pay for noise on jobs
    # that complete in milliseconds. ExecutionRecorder only records (and we only
    # publish) executions at or above this threshold (in seconds), so fast jobs
    # cost nothing — the global publisher-level metrics still cover overall
    # throughput. Set to nil to record and publish every execution.
    DEFAULT_MIN_JOB_SECONDS = 0.5

    ALL_METRICS = (
      GLOBAL_STATS_METRICS +
      GLOBAL_AGGREGATE_METRICS +
      TAG_METRICS +
      PROCESS_METRICS +
      QUEUE_METRICS +
      EXECUTION_TIME_METRICS
    ).freeze

    private def default_config
      # Sidekiq::Config was introduced in sidekiq 7 and has a default
      if Sidekiq.respond_to?(:default_configuration)
        Sidekiq.default_configuration
      else
        # in older versions, it's just the `Sidekiq` module
        Sidekiq
      end
    end

    def initialize(config: default_config, client: Aws::CloudWatch::Client.new, namespace: "Sidekiq", process_metrics: nil, additional_dimensions: {}, interval: DEFAULT_INTERVAL, metrics: nil, leader_election: nil, min_job_seconds: DEFAULT_MIN_JOB_SECONDS, sample_cap: ExecutionSamples::DEFAULT_SAMPLE_CAP, execution_samples: nil)
      # Required by Sidekiq::Component (in sidekiq 6.5+)
      @config = config

      @client = client
      @interval_s = interval
      @namespace = namespace
      @additional_dimensions = additional_dimensions.map { |k, v| {name: k.to_s, value: v.to_s} }

      @enabled_metrics = resolve_enabled_metrics(metrics, process_metrics)
      @leader = build_leader(leader_election)
      @min_job_seconds = resolve_min_job_seconds(min_job_seconds)
      @execution_sample_cap = sample_cap
      @execution_samples = execution_samples || ExecutionSamples.new(namespace: @namespace, sample_cap: @execution_sample_cap)
    end

    # Exposed so enable! can configure a matching ExecutionRecorder middleware.
    attr_reader :namespace, :min_job_seconds, :execution_sample_cap

    # True when this publisher owns any per-job execution-time percentile, i.e.
    # when the duration-recording middleware needs to run.
    def records_execution_times?
      enabled_any?(EXECUTION_TIME_METRICS)
    end

    def start
      logger.debug { "Starting Sidekiq CloudWatch Metrics Publisher" }

      @done = false
      @thread = safe_thread("cloudwatch metrics publisher", &method(:run))
    end

    def running?
      !@thread.nil? && @thread.alive?
    end

    def run
      logger.info { "Started Sidekiq CloudWatch Metrics Publisher" }

      # Publish stats every @interval_s seconds, sleeping as required between runs
      now = Time.now.to_f
      tick = now
      until @stop
        logger.debug { "Publishing Sidekiq CloudWatch Metrics" }
        begin
          publish
        rescue => e
          logger.error("Error publishing Sidekiq CloudWatch Metrics: #{e}")
          handle_exception(e)
        end

        now = Time.now.to_f
        tick = [tick + @interval_s, now].max
        sleep(tick - now) if tick > now
      end

      logger.debug { "Stopped Sidekiq CloudWatch Metrics Publisher" }
    end

    def publish
      return unless leader?

      now = Time.now
      metrics = []

      needs_stats = enabled_any?(GLOBAL_STATS_METRICS)
      needs_processes = enabled_any?(GLOBAL_AGGREGATE_METRICS + TAG_METRICS + PROCESS_METRICS)
      needs_queues = enabled_any?(QUEUE_METRICS)

      # Sidekiq::Stats already fetches queue names and sizes on init, so reuse
      # it when we need either global stats or per-queue metrics.
      stats = Sidekiq::Stats.new if needs_stats || needs_queues
      processes = needs_processes ? Sidekiq::ProcessSet.new.to_enum(:each).to_a : []

      if needs_stats
        metrics << build_metric("ProcessedJobs", stats.processed, now) if enabled?(:processed_jobs)
        metrics << build_metric("FailedJobs", stats.failed, now) if enabled?(:failed_jobs)
        metrics << build_metric("EnqueuedJobs", stats.enqueued, now) if enabled?(:enqueued_jobs)
        metrics << build_metric("ScheduledJobs", stats.scheduled_size, now) if enabled?(:scheduled_jobs)
        metrics << build_metric("RetryJobs", stats.retry_size, now) if enabled?(:retry_jobs)
        metrics << build_metric("DeadJobs", stats.dead_size, now) if enabled?(:dead_jobs)
        metrics << build_metric("Workers", stats.workers_size, now) if enabled?(:workers)
        metrics << build_metric("Processes", stats.processes_size, now) if enabled?(:processes)
        metrics << build_metric("DefaultQueueLatency", stats.default_queue_latency, now, unit: "Seconds") if enabled?(:default_queue_latency)
      end

      if enabled?(:capacity)
        metrics << build_metric("Capacity", calculate_capacity(processes), now)
      end

      if enabled?(:utilization)
        utilization = calculate_utilization(processes) * 100.0
        unless utilization.nan?
          metrics << build_metric("Utilization", utilization, now, unit: "Percent")
        end
      end

      if enabled_any?(TAG_METRICS)
        processes.group_by { |process| process["tag"] }.each do |(tag, tag_processes)|
          next if tag.nil?

          tag_dimensions = [{name: "Tag", value: tag}]

          if enabled?(:tag_capacity)
            metrics << build_metric("Capacity", calculate_capacity(tag_processes), now, dimensions: tag_dimensions)
          end

          if enabled?(:tag_utilization)
            tag_utilization = calculate_utilization(tag_processes) * 100.0

            unless tag_utilization.nan?
              metrics << build_metric("Utilization", tag_utilization, now, unit: "Percent", dimensions: tag_dimensions)
            end
          end
        end
      end

      if enabled?(:process_utilization)
        processes.each do |process|
          process_utilization = process["busy"] / process["concurrency"].to_f * 100.0

          next if process_utilization.nan?

          process_dimensions = [{name: "Hostname", value: process["hostname"]}]

          if process["tag"] && !process["tag"].to_s.empty?
            process_dimensions << {name: "Tag", value: process["tag"]}
          end

          metrics << build_metric("Utilization", process_utilization, now, unit: "Percent", dimensions: process_dimensions)
        end
      end

      if needs_queues
        stats.queues.each do |(queue_name, queue_size)|
          queue_dimensions = [{name: "QueueName", value: queue_name}]

          if enabled?(:queue_size)
            metrics << build_metric("QueueSize", queue_size, now, dimensions: queue_dimensions)
          end

          if enabled?(:queue_latency)
            queue_latency = Sidekiq::Queue.new(queue_name).latency
            metrics << build_metric("QueueLatency", queue_latency, now, unit: "Seconds", dimensions: queue_dimensions)
          end
        end
      end

      if records_execution_times?
        @execution_samples.drain(now - EXECUTION_WINDOW_SECONDS).each do |klass, durations_ms|
          next if durations_ms.empty?
          job_dimensions = [{name: "JobClass", value: klass}]
          EXECUTION_TIME_METRICS.each do |key|
            next unless enabled?(key)
            percentile, metric_name = EXECUTION_TIME_PERCENTILES.fetch(key)
            seconds = percentile_seconds(durations_ms, percentile)
            next if seconds.nil?
            metrics << build_metric(metric_name, seconds, now, unit: "Seconds", dimensions: job_dimensions)
          end
        end
      end

      unless @additional_dimensions.empty?
        metrics.each do |metric|
          metric[:dimensions] = (metric[:dimensions] || []) + @additional_dimensions
        end
      end

      if @interval_s < 60
        metrics.each { |metric| metric[:storage_resolution] = 1 }
      end

      # We can only put 20 metrics at a time
      metrics.each_slice(20) do |some_metrics|
        @client.put_metric_data(
          namespace: @namespace,
          metric_data: some_metrics,
        )
      end
    end

    private def build_metric(name, value, timestamp, unit: "Count", dimensions: nil)
      metric = {
        metric_name: name,
        timestamp: timestamp,
        value: value,
        unit: unit,
      }
      metric[:dimensions] = dimensions if dimensions
      metric
    end

    private def enabled?(key)
      @enabled_metrics.include?(key)
    end

    private def enabled_any?(keys)
      (@enabled_metrics & keys).any?
    end

    # When leader_election is configured, only the lock holder publishes on
    # each tick. Every node still runs the publisher thread so that failover
    # is automatic: the next node to acquire the lock starts publishing at
    # its next tick, no restart required.
    private def leader?
      return true if @leader.nil?
      @leader.acquire_or_extend
    end

    private def build_leader(leader_election)
      case leader_election
      when nil, false
        nil
      when :redis
        # Interval is part of the key so multiple publishers on the same
        # namespace (e.g. a 60s publisher + a 10s burst publisher) elect
        # leaders independently rather than fighting over one Redis key.
        RedisLeader.new(
          key: "sidekiq-cloudwatchmetrics:leader:#{@namespace}:#{@interval_s}s",
          interval: @interval_s,
        )
      else
        # Accept any object that quacks like a leader so callers can plug in
        # their own elector (a different backend, a test double, etc.).
        unless leader_election.respond_to?(:acquire_or_extend) && leader_election.respond_to?(:release)
          raise ArgumentError, "Unknown leader_election option: #{leader_election.inspect}. Expected :redis, nil, or an object responding to #acquire_or_extend and #release."
        end
        leader_election
      end
    end

    private def resolve_enabled_metrics(metrics, process_metrics)
      enabled =
        if metrics.nil?
          ALL_METRICS.dup
        else
          requested = Array(metrics).map(&:to_sym)
          unknown = requested - ALL_METRICS
          if unknown.any?
            raise ArgumentError, "Unknown metric#{"s" if unknown.size > 1}: #{unknown.inspect}. Available: #{ALL_METRICS.inspect}"
          end
          requested
        end

      unless process_metrics.nil?
        warn "[sidekiq-cloudwatchmetrics] `process_metrics:` is deprecated; use `metrics:` to choose which metrics to publish (omit `:process_utilization` to disable per-process utilization)."
        enabled -= [:process_utilization] unless process_metrics
      end

      enabled
    end

    private def resolve_min_job_seconds(value)
      return nil if value.nil?
      float_value = Float(value)
      return nil if float_value <= 0
      float_value
    end

    # Nearest-rank percentile (in seconds) over a class's raw execution
    # durations (milliseconds) for the drained minute. There is no ceiling:
    # a job that ran 600s reports 600.0, where the old histogram path clamped
    # everything past its top bucket to 335s.
    private def percentile_seconds(durations_ms, percentile)
      return nil if durations_ms.empty?

      sorted = durations_ms.sort
      rank = (percentile * sorted.length).ceil
      rank = 1 if rank < 1
      rank = sorted.length if rank > sorted.length
      (sorted[rank - 1] / 1000.0).round(3)
    end

    # Returns the total number of workers across all processes
    private def calculate_capacity(processes)
      processes.map do |process|
        process["concurrency"]
      end.sum
    end

    # Returns busy / concurrency averaged across processes (for scaling)
    # Avoid considering processes not yet running any threads
    private def calculate_utilization(processes)
      process_utilizations = processes.map do |process|
        process["busy"] / process["concurrency"].to_f
      end.reject(&:nan?)

      process_utilizations.sum / process_utilizations.size.to_f
    end

    def quiet
      logger.debug { "Quieting Sidekiq CloudWatch Metrics Publisher" }
      @stop = true
    end

    def stop
      logger.debug { "Stopping Sidekiq CloudWatch Metrics Publisher" }
      @stop = true
      if @thread
        @thread.wakeup
        @thread.join
      end
    rescue ThreadError
      # Don't raise if thread is already dead.
      nil
    ensure
      @leader&.release
    end
  end
end

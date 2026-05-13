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

  def self.enable!(**kwargs)
    Sidekiq.configure_server do |config|
      publisher = Publisher.new(config: config, **kwargs)

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

    # Per-job-class execution time percentiles, derived from the histogram data
    # Sidekiq 7+ records in Redis via Sidekiq::Metrics::ExecutionTracker.
    EXECUTION_HISTOGRAM_METRICS = %i[
      job_execution_time_p50
      job_execution_time_p95
      job_execution_time_p99
    ].freeze

    EXECUTION_HISTOGRAM_PERCENTILES = {
      job_execution_time_p50: [0.50, "JobExecutionTimeP50"],
      job_execution_time_p95: [0.95, "JobExecutionTimeP95"],
      job_execution_time_p99: [0.99, "JobExecutionTimeP99"],
    }.freeze

    ALL_METRICS = (
      GLOBAL_STATS_METRICS +
      GLOBAL_AGGREGATE_METRICS +
      TAG_METRICS +
      PROCESS_METRICS +
      QUEUE_METRICS +
      EXECUTION_HISTOGRAM_METRICS
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

    def initialize(config: default_config, client: Aws::CloudWatch::Client.new, namespace: "Sidekiq", process_metrics: nil, additional_dimensions: {}, interval: DEFAULT_INTERVAL, metrics: nil, leader_election: nil)
      # Required by Sidekiq::Component (in sidekiq 6.5+)
      @config = config

      @client = client
      @interval_s = interval
      @namespace = namespace
      @additional_dimensions = additional_dimensions.map { |k, v| {name: k.to_s, value: v.to_s} }

      @enabled_metrics = resolve_enabled_metrics(metrics, process_metrics)
      @leader = build_leader(leader_election)
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

      if enabled_any?(EXECUTION_HISTOGRAM_METRICS) && execution_histograms_supported?
        fetch_recent_execution_histograms.each do |klass, buckets|
          job_dimensions = [{name: "JobClass", value: klass}]
          EXECUTION_HISTOGRAM_METRICS.each do |key|
            next unless enabled?(key)
            percentile, metric_name = EXECUTION_HISTOGRAM_PERCENTILES.fetch(key)
            seconds = percentile_seconds(buckets, percentile)
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
        RedisLeader.new(key: "sidekiq-cloudwatchmetrics:leader:#{@namespace}", interval: @interval_s)
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

    # Sidekiq 7 introduced the in-process ExecutionTracker, which records
    # per-class execution time histograms in Redis. The publisher reads those
    # histograms (not raw timings) so each tick is cheap regardless of throughput.
    private def execution_histograms_supported?
      defined?(Sidekiq::Metrics::Query) && defined?(Sidekiq::Metrics::Histogram)
    end

    # The ExecutionTracker flushes to Redis on each Sidekiq heartbeat (~10s),
    # so we query the previous full minute to avoid racing an in-progress flush.
    # Returns { class_name => [bucket_count, ...] } for classes with activity.
    private def fetch_recent_execution_histograms
      query_time = Time.now - 60
      query = Sidekiq::Metrics::Query.new(now: query_time)
      result = query.top_jobs(minutes: 1)
      return {} if result.job_results.empty?

      histograms = {}
      Sidekiq.redis do |conn|
        result.job_results.each_key do |klass|
          buckets = Sidekiq::Metrics::Histogram.new(klass).fetch(conn, query_time)
          next if buckets.nil? || buckets.sum.zero?
          histograms[klass] = buckets
        end
      end
      histograms
    end

    # Computes a percentile from Sidekiq's 26-bucket execution histogram.
    # Each bucket's upper bound comes from Sidekiq::Metrics::Histogram::BUCKET_INTERVALS;
    # we report that upper bound (a conservative over-estimate). The final
    # "Slow" bucket has an effectively infinite upper bound, so we clamp it to
    # the previous bucket's upper bound to keep the published value plottable.
    private def percentile_seconds(buckets, percentile)
      total = buckets.sum
      return nil if total.zero?

      intervals = Sidekiq::Metrics::Histogram::BUCKET_INTERVALS
      last_index = intervals.size - 1
      target = total * percentile
      cumulative = 0
      buckets.each_with_index do |count, idx|
        cumulative += count
        next if cumulative < target
        upper_ms = (idx == last_index) ? intervals[last_index - 1] : intervals[idx]
        return upper_ms / 1000.0
      end
      nil
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

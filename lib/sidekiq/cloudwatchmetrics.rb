# frozen_string_literal: true

require "sidekiq"
require "sidekiq/api"

require "aws-sdk-cloudwatch"

module Sidekiq::CloudWatchMetrics
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

    ALL_METRICS = (
      GLOBAL_STATS_METRICS +
      GLOBAL_AGGREGATE_METRICS +
      TAG_METRICS +
      PROCESS_METRICS +
      QUEUE_METRICS
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

    def initialize(config: default_config, client: Aws::CloudWatch::Client.new, namespace: "Sidekiq", process_metrics: nil, additional_dimensions: {}, interval: DEFAULT_INTERVAL, metrics: nil)
      # Required by Sidekiq::Component (in sidekiq 6.5+)
      @config = config

      @client = client
      @interval_s = interval
      @namespace = namespace
      @additional_dimensions = additional_dimensions.map { |k, v| {name: k.to_s, value: v.to_s} }

      @enabled_metrics = resolve_enabled_metrics(metrics, process_metrics)
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
    end
  end
end

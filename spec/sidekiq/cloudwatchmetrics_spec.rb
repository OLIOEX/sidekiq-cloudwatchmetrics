require "spec_helper"

require "fiber"

RSpec.describe Sidekiq::CloudWatchMetrics do
  describe ".enable!" do
    # Sidekiq.options is deprecated as of Sidekiq 6.5, and must be accessed
    # through Sidekiq[...] instead. In Sidekiq 7.0 we must use Sidekiq::Config.new
    let(:sidekiq_options) do
      return Sidekiq if Sidekiq.respond_to?(:[])
      return Sidekiq.options if Sidekiq.respond_to?(:options)

      Sidekiq::Config.new
    end

    # Sidekiq.options does a Sidekiq::DEFAULTS.dup which retains the same values, so
    # Sidekiq.options[:lifecycle_events] IS Sidekiq::DEFAULTS[:lifecycle_events] and
    # is mutable, so Sidekiq.options = nil will again Sidekiq::DEFAULTS.dup and get
    # the same Sidekiq::DEFAULTS[:lifecycle_events]. So we have to manually clear it.
    before { sidekiq_options[:lifecycle_events].each_value(&:clear) }

    context "in a sidekiq server" do
      before { allow(Sidekiq).to receive(:server?).and_return(true) }

      it "creates a metrics publisher and installs hooks" do
        publisher = instance_double(Sidekiq::CloudWatchMetrics::Publisher)
        expect(Sidekiq::CloudWatchMetrics::Publisher).to receive(:new).and_return(publisher)

        Sidekiq::CloudWatchMetrics.enable!

        # Look, this is hard.
        expect(sidekiq_options[:lifecycle_events][:startup]).not_to be_empty
        expect(sidekiq_options[:lifecycle_events][:quiet]).not_to be_empty
        expect(sidekiq_options[:lifecycle_events][:shutdown]).not_to be_empty
      end
    end

    context "in client mode" do
      before { allow(Sidekiq).to receive(:server?).and_return(false) }

      it "does nothing" do
        expect(Sidekiq::CloudWatchMetrics::Publisher).not_to receive(:new)

        Sidekiq::CloudWatchMetrics.enable!

        expect(sidekiq_options[:lifecycle_events][:startup]).to be_empty
        expect(sidekiq_options[:lifecycle_events][:quiet]).to be_empty
        expect(sidekiq_options[:lifecycle_events][:shutdown]).to be_empty
      end
    end
  end

  describe "Publisher" do
    let(:client) { instance_double(Aws::CloudWatch::Client) }
    before { allow(client).to receive(:put_metric_data) }

    subject(:publisher) { Sidekiq::CloudWatchMetrics::Publisher.new(client: client) }

    describe "#run" do
      it "publishes metrics until stopped" do
        allow(publisher).to receive(:sleep) { |seconds| Fiber.yield(:sleep, seconds) }
        allow(publisher).to receive(:publish) { Fiber.yield(:publish) }

        fiber = Fiber.new { publisher.run }
        expect(fiber.resume).to eql(:publish)
        expect(fiber.resume).to match([:sleep, be_a_kind_of(Numeric) & (be < Sidekiq::CloudWatchMetrics::Publisher::DEFAULT_INTERVAL)])
        expect(fiber.resume).to eql(:publish)
        expect(fiber.resume).to match([:sleep, be_a_kind_of(Numeric) & (be < (Sidekiq::CloudWatchMetrics::Publisher::DEFAULT_INTERVAL * 2))])

        publisher.stop
        fiber.resume
        expect(fiber).not_to be_alive
      end

      context "with a custom interval" do
        subject(:publisher) { Sidekiq::CloudWatchMetrics::Publisher.new(client: client, interval: 30) }

        it "respects a custom interval" do
          allow(publisher).to receive(:sleep) { |seconds| Fiber.yield(:sleep, seconds) }
          allow(publisher).to receive(:publish) { Fiber.yield(:publish) }

          fiber = Fiber.new { publisher.run }
          expect(fiber.resume).to eql(:publish)
          expect(fiber.resume).to match([:sleep, be_a_kind_of(Numeric) & (be < 30)])

          publisher.stop
          fiber.resume
          expect(fiber).not_to be_alive
        end
      end

      it "survives an error raised during publishing" do
        allow(publisher).to receive(:sleep) { |seconds| Fiber.yield(:sleep) }

        exception = RuntimeError.new("oh no")
        allow(publisher).to receive(:publish).and_invoke(lambda { raise exception }, lambda { Fiber.yield(:publish) })
        allow(publisher).to receive(:handle_exception) { |exception| Fiber.yield(:exception, exception) }

        fiber = Fiber.new { publisher.run }
        expect(fiber.resume).to eql([:exception, exception])
        expect(fiber.resume).to eql(:sleep)
        expect(fiber.resume).to eql(:publish)
        expect(fiber.resume).to eql(:sleep)

        publisher.stop
        fiber.resume
        expect(fiber).not_to be_alive
      end
    end

    describe "#publish" do
      let(:stats) do
        instance_double(Sidekiq::Stats,
          processed: 123,
          failed: 456,
          enqueued: 6,
          scheduled_size: 1,
          retry_size: 2,
          dead_size: 3,
          queues: queues.transform_values(&:size),
          workers_size: 10,
          processes_size: 5,
          default_queue_latency: 1.23,
        )
      end
      let(:processes) do
        [
          Sidekiq::Process.new("busy" => 5, "concurrency" => 10, "hostname" => "foo"),
          Sidekiq::Process.new("busy" => 2, "concurrency" => 20, "hostname" => "bar"),
        ]
      end
      let(:queues) do
        {
          "foo" => double(size: 1, latency: 1.23),
          "bar" => double(size: 2, latency: 1.23),
        }
      end

      before do
        allow(Sidekiq::Stats).to receive(:new).and_return(stats)
        allow(Sidekiq::ProcessSet).to receive(:new).and_return(processes)
        allow(Sidekiq::Queue).to receive(:new) { |name| queues.fetch(name) }
        # By default neutralize the execution-histogram code path so existing
        # tests don't need to know about it. The dedicated context below
        # overrides these stubs to exercise the percentile calculation.
        if defined?(Sidekiq::Metrics::Query)
          allow(Sidekiq::Metrics::Query).to receive(:new).and_return(
            double(top_jobs: double(job_results: {})),
          )
        end
      end

      it "publishes sidekiq metrics to cloudwatch" do
        Timecop.freeze(now = Time.now) do
          publisher.publish

          expect(client).to have_received(:put_metric_data).with(
            namespace: "Sidekiq",
            metric_data: contain_exactly(
              {
                metric_name: "ProcessedJobs",
                timestamp: now,
                value: 123,
                unit: "Count",
              },
              {
                metric_name: "FailedJobs",
                timestamp: now,
                value: 456,
                unit: "Count",
              },
              {
                metric_name: "EnqueuedJobs",
                timestamp: now,
                value: 6,
                unit: "Count",
              },
              {
                metric_name: "ScheduledJobs",
                timestamp: now,
                value: 1,
                unit: "Count",
              },
              {
                metric_name: "RetryJobs",
                timestamp: now,
                value: 2,
                unit: "Count",
              },
              {
                metric_name: "DeadJobs",
                timestamp: now,
                value: 3,
                unit: "Count",
              },
              {
                metric_name: "Workers",
                timestamp: now,
                value: 10,
                unit: "Count",
              },
              {
                metric_name: "Processes",
                timestamp: now,
                value: 5,
                unit: "Count",
              },
              {
                metric_name: "Capacity",
                timestamp: now,
                value: 30,
                unit: "Count",
              },
              {
                metric_name: "DefaultQueueLatency",
                timestamp: now,
                value: 1.23,
                unit: "Seconds",
              },
              {
                metric_name: "Utilization",
                timestamp: now,
                value: 30.0,
                unit: "Percent",
              },
              {
                metric_name: "Utilization",
                dimensions: [{name: "Hostname", value: "foo"}],
                timestamp: now,
                unit: "Percent",
                value: 50.0,
              },
              {
                metric_name: "Utilization",
                dimensions: [{name: "Hostname", value: "bar"}],
                timestamp: now,
                unit: "Percent",
                value: 10.0,
              },
              {
                metric_name: "QueueSize",
                dimensions: [{name: "QueueName", value: "foo"}],
                timestamp: now,
                value: 1,
                unit: "Count",
              },
              {
                metric_name: "QueueLatency",
                dimensions: [{name: "QueueName", value: "foo"}],
                timestamp: now,
                value: 1.23,
                unit: "Seconds",
              },
              {
                metric_name: "QueueSize",
                dimensions: [{name: "QueueName", value: "bar"}],
                timestamp: now,
                value: 2,
                unit: "Count",
              },
              {
                metric_name: "QueueLatency",
                dimensions: [{name: "QueueName", value: "bar"}],
                timestamp: now,
                value: 1.23,
                unit: "Seconds",
              },
            ),
          )
        end
      end

      context "with a custom interval of less than 60" do
        subject(:publisher) { Sidekiq::CloudWatchMetrics::Publisher.new(client: client, interval: 30) }

        it "passes sets a storage resolution of 1" do
          Timecop.freeze(now = Time.now) do
            publisher.publish

            expect(client).to have_received(:put_metric_data).with(
              namespace: "Sidekiq",
              metric_data: including(
                {
                  metric_name: "ProcessedJobs",
                  timestamp: now,
                  value: 123,
                  unit: "Count",
                  storage_resolution: 1,
                },
                {
                  metric_name: "FailedJobs",
                  timestamp: now,
                  value: 456,
                  unit: "Count",
                  storage_resolution: 1,
                },
                {
                  metric_name: "QueueSize",
                  dimensions: [{name: "QueueName", value: "bar"}],
                  timestamp: now,
                  value: 2,
                  unit: "Count",
                  storage_resolution: 1,
                },
                {
                  metric_name: "QueueLatency",
                  dimensions: [{name: "QueueName", value: "bar"}],
                  timestamp: now,
                  value: 1.23,
                  unit: "Seconds",
                  storage_resolution: 1,
                },
              )
            )
          end
        end
      end

      context "with a custom interval of more than 60" do
        subject(:publisher) { Sidekiq::CloudWatchMetrics::Publisher.new(client: client, interval: 120) }

        it "doesn't pass a storage resolution" do
          Timecop.freeze(now = Time.now) do
            publisher.publish

            expect(client).to have_received(:put_metric_data).with(
              namespace: "Sidekiq",
              metric_data: including(
                {
                  metric_name: "ProcessedJobs",
                  timestamp: now,
                  value: 123,
                  unit: "Count",
                },
                {
                  metric_name: "FailedJobs",
                  timestamp: now,
                  value: 456,
                  unit: "Count",
                },
                {
                  metric_name: "QueueSize",
                  dimensions: [{name: "QueueName", value: "bar"}],
                  timestamp: now,
                  value: 2,
                  unit: "Count",
                },
                {
                  metric_name: "QueueLatency",
                  dimensions: [{name: "QueueName", value: "bar"}],
                  timestamp: now,
                  value: 1.23,
                  unit: "Seconds",
                },
              )
            )
          end
        end
      end

      context "with lots of queues" do
        let(:queues) { 10.times.each_with_object({}) { |i, hash| hash["queue#{i}"] = double(size: 1, latency: 1.23) } }

        it "publishes sidekiq metrics to cloudwatch in batches of 20" do
          Timecop.freeze(now = Time.now) do
            publisher.publish

            expect(client).to have_received(:put_metric_data) { |metrics|
              expect(metrics[:metric_data].size).to be <= 20
            }.at_least(:twice)
          end
        end
      end

      context "with process tags" do
        let(:processes) do
          [
            Sidekiq::Process.new("busy" => 5, "concurrency" => 5, "hostname" => "foo", "tag" => "default"),
            Sidekiq::Process.new("busy" => 2, "concurrency" => 20, "hostname" => "bar", "tag" => "shard-one"),
            Sidekiq::Process.new("busy" => 0, "concurrency" => 5, "hostname" => "baz", "tag" => "default"),
          ]
        end

        it "publishes metrics including tag as a dimension" do
          Timecop.freeze(now = Time.now) do
            publisher.publish

            expect(client).to have_received(:put_metric_data).with(
              namespace: "Sidekiq",
              metric_data: include(
                {
                  metric_name: "Utilization",
                  timestamp: now,
                  value: 36.66666666666667,
                  unit: "Percent",
                },
                {
                  metric_name: "Capacity",
                  dimensions: [{name: "Tag", value: "default"}],
                  timestamp: now,
                  unit: "Count",
                  value: 10,
                },
                {
                  metric_name: "Utilization",
                  dimensions: [{name: "Tag", value: "default"}],
                  timestamp: now,
                  value: 50.0,
                  unit: "Percent",
                },
                {
                  metric_name: "Capacity",
                  dimensions: [{name: "Tag", value: "shard-one"}],
                  timestamp: now,
                  unit: "Count",
                  value: 20,
                },
                {
                  metric_name: "Utilization",
                  dimensions: [{name: "Tag", value: "shard-one"}],
                  timestamp: now,
                  value: 10.0,
                  unit: "Percent",
                },
                {
                  metric_name: "Utilization",
                  dimensions: [{name: "Hostname", value: "foo"}, {name: "Tag", value: "default"}],
                  timestamp: now,
                  unit: "Percent",
                  value: 100.0,
                },
                {
                  metric_name: "Utilization",
                  dimensions: [{name: "Hostname", value: "bar"}, {name: "Tag", value: "shard-one"}],
                  timestamp: now,
                  unit: "Percent",
                  value: 10.0,
                },
                {
                  metric_name: "Utilization",
                  dimensions: [{name: "Hostname", value: "baz"}, {name: "Tag", value: "default"}],
                  timestamp: now,
                  unit: "Percent",
                  value: 0.0,
                },
              ),
            )
          end
        end
      end

      context "with custom dimensions" do
        subject(:publisher) { Sidekiq::CloudWatchMetrics::Publisher.new(client: client, additional_dimensions: {appCluster: 1, type: "foo"}) }

        it "publishes metrics with custom dimensions" do
          Timecop.freeze(now = Time.now) do
            publisher.publish

            expect(client).to have_received(:put_metric_data) { |metrics|
              metrics[:metric_data].each do |metric|
                expect(metric[:dimensions]).to include({name: "appCluster", value: "1"}, {name: "type", value: "foo"})
              end
            }.at_least(:once)
          end
        end
      end

      context "with a custom namespace" do
        subject(:publisher) { Sidekiq::CloudWatchMetrics::Publisher.new(client: client, namespace: "Sidekiq-Test") }

        it "publishes metrics with the specified namespace" do
          Timecop.freeze(now = Time.now) do
            publisher.publish

            expect(client).to have_received(:put_metric_data) { |metrics|
              expect(metrics[:namespace]).to eql("Sidekiq-Test")
            }.at_least(:once)
          end
        end
      end

      context "when there are no processes yet" do
        let(:processes) { [] }

        it "does not publish Utilization (to avoid NaN values)" do
          Timecop.freeze(now = Time.now) do
            publisher.publish

            expect(client).to have_received(:put_metric_data) { |metrics|
              expect(metrics[:metric_data]).not_to include(hash_including(metric_name: "Utilization"))
            }
          end
        end
      end

      context "when the only process has no threads yet" do
        let(:processes) { [Sidekiq::Process.new("busy" => 0, "concurrency" => 0, "hostname" => "foo")] }

        it "does not publish Utilization (to avoid NaN values)" do
          Timecop.freeze(now = Time.now) do
            publisher.publish

            expect(client).to have_received(:put_metric_data) { |metrics|
              expect(metrics[:metric_data]).not_to include(hash_including(metric_name: "Utilization"))
            }
          end
        end
      end

      context "when only one process has no threads yet" do
        let(:processes) { [
          Sidekiq::Process.new("busy" => 0, "concurrency" => 0, "hostname" => "foo"),
          Sidekiq::Process.new("busy" => 2, "concurrency" => 4, "hostname" => "bar"),
        ] }

        it "publishes partial Utilization (to avoid NaN values)" do
          Timecop.freeze(now = Time.now) do
            publisher.publish

            expect(client).to have_received(:put_metric_data) { |metrics|
              utilization_data = metrics[:metric_data].select { |data| data[:metric_name] == "Utilization" }

              expect(utilization_data).to contain_exactly(
                {
                  metric_name: "Utilization",
                  timestamp: now,
                  value: 50.0,
                  unit: "Percent",
                },
                {
                  metric_name: "Utilization",
                  dimensions: [{name: "Hostname", value: "bar"}],
                  timestamp: now,
                  unit: "Percent",
                  value: 50.0,
                },
              )
            }
          end
        end
      end

      context "when per process metrics are disabled" do
        subject(:publisher) do
          # Silence the deprecation warning emitted at init time
          original_stderr = $stderr
          $stderr = StringIO.new
          begin
            Sidekiq::CloudWatchMetrics::Publisher.new(client: client, process_metrics: false)
          ensure
            $stderr = original_stderr
          end
        end

        it "only publishes a single Utilization metric" do
          Timecop.freeze(now = Time.now) do
            publisher.publish

            expect(client).to have_received(:put_metric_data) { |metrics|
              utilization_data = metrics[:metric_data].select { |data| data[:metric_name] == "Utilization" }

              expect(utilization_data).to contain_exactly(
                {
                  metric_name: "Utilization",
                  timestamp: now,
                  value: 30.0,
                  unit: "Percent",
                },
              )
            }
          end
        end

        it "emits a deprecation warning" do
          expect {
            Sidekiq::CloudWatchMetrics::Publisher.new(client: client, process_metrics: false)
          }.to output(/`process_metrics:` is deprecated/).to_stderr
        end
      end

      context "with a metrics filter" do
        context "selecting only a single global metric" do
          subject(:publisher) { Sidekiq::CloudWatchMetrics::Publisher.new(client: client, metrics: [:retry_jobs]) }

          it "publishes only that metric" do
            Timecop.freeze(now = Time.now) do
              publisher.publish

              expect(client).to have_received(:put_metric_data).with(
                namespace: "Sidekiq",
                metric_data: [
                  {
                    metric_name: "RetryJobs",
                    timestamp: now,
                    value: 2,
                    unit: "Count",
                  },
                ],
              )
            end
          end

          it "does not enumerate processes or queues" do
            expect(Sidekiq::ProcessSet).not_to receive(:new)
            expect(Sidekiq::Queue).not_to receive(:new)
            publisher.publish
          end
        end

        context "selecting only queue metrics" do
          subject(:publisher) do
            Sidekiq::CloudWatchMetrics::Publisher.new(client: client, metrics: %i[queue_size queue_latency])
          end

          it "publishes only the queue metrics, with high-resolution storage when interval < 60" do
            publisher_with_short_interval = Sidekiq::CloudWatchMetrics::Publisher.new(
              client: client, metrics: %i[queue_size queue_latency], interval: 10
            )

            Timecop.freeze(now = Time.now) do
              publisher_with_short_interval.publish

              expect(client).to have_received(:put_metric_data).with(
                namespace: "Sidekiq",
                metric_data: contain_exactly(
                  {metric_name: "QueueSize", dimensions: [{name: "QueueName", value: "foo"}], timestamp: now, value: 1, unit: "Count", storage_resolution: 1},
                  {metric_name: "QueueLatency", dimensions: [{name: "QueueName", value: "foo"}], timestamp: now, value: 1.23, unit: "Seconds", storage_resolution: 1},
                  {metric_name: "QueueSize", dimensions: [{name: "QueueName", value: "bar"}], timestamp: now, value: 2, unit: "Count", storage_resolution: 1},
                  {metric_name: "QueueLatency", dimensions: [{name: "QueueName", value: "bar"}], timestamp: now, value: 1.23, unit: "Seconds", storage_resolution: 1},
                ),
              )
            end
          end

          it "does not enumerate processes" do
            expect(Sidekiq::ProcessSet).not_to receive(:new)
            publisher.publish
          end
        end

        context "selecting only process_utilization" do
          subject(:publisher) { Sidekiq::CloudWatchMetrics::Publisher.new(client: client, metrics: [:process_utilization]) }

          it "does not call Sidekiq::Stats" do
            expect(Sidekiq::Stats).not_to receive(:new)
            publisher.publish
          end

          it "publishes only the per-process utilization metrics" do
            Timecop.freeze(now = Time.now) do
              publisher.publish

              expect(client).to have_received(:put_metric_data).with(
                namespace: "Sidekiq",
                metric_data: contain_exactly(
                  {metric_name: "Utilization", dimensions: [{name: "Hostname", value: "foo"}], timestamp: now, value: 50.0, unit: "Percent"},
                  {metric_name: "Utilization", dimensions: [{name: "Hostname", value: "bar"}], timestamp: now, value: 10.0, unit: "Percent"},
                ),
              )
            end
          end
        end

        context "selecting job execution time percentiles", if: defined?(Sidekiq::Metrics::Query) do
          subject(:publisher) do
            Sidekiq::CloudWatchMetrics::Publisher.new(
              client: client,
              metrics: %i[job_execution_time_p50 job_execution_time_p95 job_execution_time_p99],
            )
          end

          let(:fake_conn) { double(:redis_conn) }

          # 26-bucket histograms, matching Sidekiq::Metrics::Histogram::BUCKET_INTERVALS.
          #   FastJob: 100 samples in bucket 0 → every percentile lands at 20ms.
          let(:fast_job_buckets) { [100] + Array.new(25, 0) }

          # SlowJob: 90 in bucket 0 (≤20ms), 8 in bucket 10 (≤1.1s), 2 in bucket 17 (≤20s).
          #   p50 → cumulative 90 at idx 0  → 20ms   = 0.02s
          #   p95 → cumulative 98 at idx 10 → 1100ms = 1.1s
          #   p99 → cumulative 100 at idx 17 → 20000ms = 20s
          let(:slow_job_buckets) do
            Array.new(26, 0).tap do |buckets|
              buckets[0]  = 90
              buckets[10] = 8
              buckets[17] = 2
            end
          end

          before do
            job_results = {"FastJob" => double(:fast_result), "SlowJob" => double(:slow_result)}
            allow(Sidekiq::Metrics::Query).to receive(:new).and_return(
              double(top_jobs: double(job_results: job_results)),
            )

            allow(Sidekiq).to receive(:redis).and_yield(fake_conn)

            fast_hist = double(:fast_histogram, fetch: fast_job_buckets)
            slow_hist = double(:slow_histogram, fetch: slow_job_buckets)
            allow(Sidekiq::Metrics::Histogram).to receive(:new).with("FastJob").and_return(fast_hist)
            allow(Sidekiq::Metrics::Histogram).to receive(:new).with("SlowJob").and_return(slow_hist)
          end

          it "publishes per-class p50/p95/p99 derived from the execution histograms" do
            Timecop.freeze(now = Time.now) do
              publisher.publish

              expect(client).to have_received(:put_metric_data).with(
                namespace: "Sidekiq",
                metric_data: contain_exactly(
                  {metric_name: "JobExecutionTimeP50", dimensions: [{name: "JobClass", value: "FastJob"}], timestamp: now, value: 0.02, unit: "Seconds"},
                  {metric_name: "JobExecutionTimeP95", dimensions: [{name: "JobClass", value: "FastJob"}], timestamp: now, value: 0.02, unit: "Seconds"},
                  {metric_name: "JobExecutionTimeP99", dimensions: [{name: "JobClass", value: "FastJob"}], timestamp: now, value: 0.02, unit: "Seconds"},
                  {metric_name: "JobExecutionTimeP50", dimensions: [{name: "JobClass", value: "SlowJob"}], timestamp: now, value: 0.02, unit: "Seconds"},
                  {metric_name: "JobExecutionTimeP95", dimensions: [{name: "JobClass", value: "SlowJob"}], timestamp: now, value: 1.1, unit: "Seconds"},
                  {metric_name: "JobExecutionTimeP99", dimensions: [{name: "JobClass", value: "SlowJob"}], timestamp: now, value: 20.0, unit: "Seconds"},
                ),
              )
            end
          end

          it "does not call Sidekiq::Stats, ProcessSet, or Queue" do
            expect(Sidekiq::Stats).not_to receive(:new)
            expect(Sidekiq::ProcessSet).not_to receive(:new)
            expect(Sidekiq::Queue).not_to receive(:new)
            publisher.publish
          end

          context "when the percentile falls into the final \"Slow\" bucket" do
            let(:slow_job_buckets) { Array.new(25, 0) + [5] }

            it "clamps to the previous bucket's upper bound to keep the value plottable" do
              # BUCKET_INTERVALS[24] = 335000 → 335.0s
              Timecop.freeze(now = Time.now) do
                publisher.publish

                expect(client).to have_received(:put_metric_data).with(
                  namespace: "Sidekiq",
                  metric_data: include(
                    {metric_name: "JobExecutionTimeP99", dimensions: [{name: "JobClass", value: "SlowJob"}], timestamp: now, value: 335.0, unit: "Seconds"},
                  ),
                )
              end
            end
          end

          context "with a class that has no recorded activity" do
            let(:slow_job_buckets) { Array.new(26, 0) }

            it "publishes nothing for that class" do
              Timecop.freeze(now = Time.now) do
                publisher.publish

                expect(client).to have_received(:put_metric_data).with(
                  namespace: "Sidekiq",
                  metric_data: contain_exactly(
                    {metric_name: "JobExecutionTimeP50", dimensions: [{name: "JobClass", value: "FastJob"}], timestamp: now, value: 0.02, unit: "Seconds"},
                    {metric_name: "JobExecutionTimeP95", dimensions: [{name: "JobClass", value: "FastJob"}], timestamp: now, value: 0.02, unit: "Seconds"},
                    {metric_name: "JobExecutionTimeP99", dimensions: [{name: "JobClass", value: "FastJob"}], timestamp: now, value: 0.02, unit: "Seconds"},
                  ),
                )
              end
            end
          end
        end

        context "when Sidekiq::Metrics is not available" do
          subject(:publisher) do
            Sidekiq::CloudWatchMetrics::Publisher.new(client: client, metrics: [:job_execution_time_p99])
          end

          it "silently skips the execution-histogram metrics" do
            allow(publisher).to receive(:execution_histograms_supported?).and_return(false)

            publisher.publish

            expect(client).not_to have_received(:put_metric_data)
          end
        end

        context "capping JobClass cardinality", if: defined?(Sidekiq::Metrics::Query) do
          let(:fake_conn) { double(:redis_conn) }

          # Three buckets per class so the test stays readable; cap_job_class_cardinality
          # uses transpose+sum so it works for any histogram width.
          let(:job_buckets) do
            {
              "BusiestJob" => [50, 0, 0],
              "MidJob"     => [30, 0, 0],
              "RareJobA"   => [3,  0, 0],
              "RareJobB"   => [1,  2, 0],
            }
          end

          before do
            job_results = job_buckets.transform_values { |_| double(:result) }
            allow(Sidekiq::Metrics::Query).to receive(:new).and_return(
              double(top_jobs: double(job_results: job_results)),
            )
            allow(Sidekiq).to receive(:redis).and_yield(fake_conn)
            job_buckets.each do |klass, buckets|
              histogram = double(:"#{klass}_histogram", fetch: buckets)
              allow(Sidekiq::Metrics::Histogram).to receive(:new).with(klass).and_return(histogram)
            end
            # Stub the percentile calculation to make assertions about which
            # classes get published — the bucket maths is covered elsewhere.
            allow_any_instance_of(Sidekiq::CloudWatchMetrics::Publisher)
              .to receive(:percentile_seconds) { |_pub, buckets, _pct| buckets.sum.to_f }
          end

          subject(:publisher) do
            Sidekiq::CloudWatchMetrics::Publisher.new(
              client: client, metrics: [:job_execution_time_p99], max_job_classes: 2,
            )
          end

          it "keeps the busiest classes and rolls the rest into an (other) series" do
            publisher.publish

            expect(client).to have_received(:put_metric_data) do |args|
              job_classes = args[:metric_data].map { |m| m[:dimensions].first[:value] }
              expect(job_classes).to contain_exactly("BusiestJob", "MidJob", "(other)")

              other = args[:metric_data].find { |m| m[:dimensions].first[:value] == "(other)" }
              # RareJobA (sum=3) + RareJobB (sum=3) → (other) sum = 6
              expect(other[:value]).to eq(6.0)
            end
          end

          context "with max_job_classes: nil (uncapped)" do
            subject(:publisher) do
              Sidekiq::CloudWatchMetrics::Publisher.new(
                client: client, metrics: [:job_execution_time_p99], max_job_classes: nil,
              )
            end

            it "publishes one series per class with no rollup bucket" do
              publisher.publish

              expect(client).to have_received(:put_metric_data) do |args|
                job_classes = args[:metric_data].map { |m| m[:dimensions].first[:value] }
                expect(job_classes).to contain_exactly("BusiestJob", "MidJob", "RareJobA", "RareJobB")
              end
            end
          end

          context "when the number of classes is at or below the cap" do
            subject(:publisher) do
              Sidekiq::CloudWatchMetrics::Publisher.new(
                client: client, metrics: [:job_execution_time_p99], max_job_classes: 10,
              )
            end

            it "publishes one series per class with no rollup bucket" do
              publisher.publish

              expect(client).to have_received(:put_metric_data) do |args|
                job_classes = args[:metric_data].map { |m| m[:dimensions].first[:value] }
                expect(job_classes).not_to include("(other)")
                expect(job_classes.size).to eq(job_buckets.size)
              end
            end
          end

          context "when two tail classes tie on sample count" do
            let(:job_buckets) do
              {
                "BusiestJob" => [50, 0, 0],
                "MidJob"     => [30, 0, 0],
                "TieJobZ"    => [5,  0, 0],
                "TieJobA"    => [5,  0, 0],
              }
            end

            it "breaks ties by class name so borderline classes don't flap across cycles" do
              # max_job_classes: 3 keeps BusiestJob, MidJob, and one of the
              # tied pair. Without a stable secondary sort key the survivor
              # would depend on hash ordering.
              publisher = Sidekiq::CloudWatchMetrics::Publisher.new(
                client: client, metrics: [:job_execution_time_p99], max_job_classes: 3,
              )

              publisher.publish

              expect(client).to have_received(:put_metric_data) do |args|
                job_classes = args[:metric_data].map { |m| m[:dimensions].first[:value] }
                # Ascending class name wins the tie → TieJobA survives,
                # TieJobZ rolls into (other).
                expect(job_classes).to contain_exactly(
                  "BusiestJob", "MidJob", "TieJobA", "(other)",
                )
              end
            end
          end
        end

        context "with max_job_classes set to a non-positive value" do
          it "raises ArgumentError at init" do
            expect {
              Sidekiq::CloudWatchMetrics::Publisher.new(client: client, max_job_classes: 0)
            }.to raise_error(ArgumentError, /max_job_classes must be positive/)
          end
        end

        context "with an unknown metric" do
          it "raises ArgumentError at init" do
            expect {
              Sidekiq::CloudWatchMetrics::Publisher.new(client: client, metrics: [:not_a_real_metric])
            }.to raise_error(ArgumentError, /Unknown metric.*:not_a_real_metric/)
          end
        end

        context "with an empty metrics array" do
          subject(:publisher) { Sidekiq::CloudWatchMetrics::Publisher.new(client: client, metrics: []) }

          it "publishes nothing" do
            publisher.publish
            expect(client).not_to have_received(:put_metric_data)
          end
        end
      end

      context "with leader_election" do
        let(:leader_acquired) { true }
        let(:leader) do
          instance_double(Sidekiq::CloudWatchMetrics::RedisLeader, acquire_or_extend: leader_acquired, release: nil)
        end
        subject(:publisher) do
          Sidekiq::CloudWatchMetrics::Publisher.new(client: client, leader_election: leader)
        end

        context "when this process holds the leader lock" do
          let(:leader_acquired) { true }

          it "publishes as normal" do
            publisher.publish
            expect(client).to have_received(:put_metric_data)
          end
        end

        context "when another process holds the leader lock" do
          let(:leader_acquired) { false }

          it "skips publishing entirely" do
            publisher.publish
            expect(client).not_to have_received(:put_metric_data)
          end

          it "does not enumerate stats, processes, or queues" do
            expect(Sidekiq::Stats).not_to receive(:new)
            expect(Sidekiq::ProcessSet).not_to receive(:new)
            expect(Sidekiq::Queue).not_to receive(:new)
            publisher.publish
          end
        end

        context "with leader_election: :redis" do
          it "builds a RedisLeader scoped by namespace and interval" do
            expect(Sidekiq::CloudWatchMetrics::RedisLeader).to receive(:new).with(
              key: "sidekiq-cloudwatchmetrics:leader:my-ns:60s",
              interval: 60,
            ).and_return(leader)

            Sidekiq::CloudWatchMetrics::Publisher.new(
              client: client, namespace: "my-ns", leader_election: :redis,
            )
          end

          it "uses distinct keys for publishers on the same namespace with different intervals" do
            keys = []
            allow(Sidekiq::CloudWatchMetrics::RedisLeader).to receive(:new) { |args| keys << args[:key]; leader }

            Sidekiq::CloudWatchMetrics::Publisher.new(
              client: client, namespace: "my-ns", interval: 60, leader_election: :redis,
            )
            Sidekiq::CloudWatchMetrics::Publisher.new(
              client: client, namespace: "my-ns", interval: 10, leader_election: :redis,
            )

            expect(keys).to eq([
              "sidekiq-cloudwatchmetrics:leader:my-ns:60s",
              "sidekiq-cloudwatchmetrics:leader:my-ns:10s",
            ])
          end
        end

        context "with an unknown leader_election value" do
          it "raises ArgumentError at init" do
            expect {
              Sidekiq::CloudWatchMetrics::Publisher.new(client: client, leader_election: :etcd)
            }.to raise_error(ArgumentError, /Unknown leader_election option: :etcd/)
          end
        end
      end
    end

    describe "#stop" do
      it "doesn't raise ThreadError" do
        thread = double("thread", wakeup: true, join: true)
        allow(thread).to receive(:wakeup).and_raise(ThreadError)
        publisher.instance_variable_set("@thread", thread)

        expect do
          publisher.stop
        end.not_to raise_error
      end

      it "releases the leader lock when configured" do
        leader = instance_double(Sidekiq::CloudWatchMetrics::RedisLeader, acquire_or_extend: true)
        expect(leader).to receive(:release)
        publisher = Sidekiq::CloudWatchMetrics::Publisher.new(client: client, leader_election: leader)
        publisher.stop
      end
    end
  end

  describe Sidekiq::CloudWatchMetrics::RedisLeader do
    let(:conn) { double(:redis_conn) }

    subject(:leader) { described_class.new(key: "test-lock", interval: 60, id: "node-a") }

    before { allow(Sidekiq).to receive(:redis).and_yield(conn) }

    describe "#acquire_or_extend" do
      it "acquires when the lock is free" do
        expect(conn).to receive(:set).with("test-lock", "node-a", nx: true, ex: 180).and_return(true)
        expect(leader.acquire_or_extend).to be true
      end

      it "extends the TTL when this process already holds the lock" do
        expect(conn).to receive(:set).with("test-lock", "node-a", nx: true, ex: 180).and_return(false)
        expect(conn).to receive(:get).with("test-lock").and_return("node-a")
        expect(conn).to receive(:expire).with("test-lock", 180).and_return(true)
        expect(leader.acquire_or_extend).to be true
      end

      it "returns false when another process holds the lock" do
        expect(conn).to receive(:set).with("test-lock", "node-a", nx: true, ex: 180).and_return(false)
        expect(conn).to receive(:get).with("test-lock").and_return("node-b")
        expect(conn).not_to receive(:expire)
        expect(leader.acquire_or_extend).to be false
      end
    end

    describe "#release" do
      it "deletes the lock when this process holds it" do
        expect(conn).to receive(:get).with("test-lock").and_return("node-a")
        expect(conn).to receive(:del).with("test-lock")
        leader.release
      end

      it "leaves the lock alone when held by another process" do
        expect(conn).to receive(:get).with("test-lock").and_return("node-b")
        expect(conn).not_to receive(:del)
        leader.release
      end

      it "swallows errors so shutdown is never blocked by Redis hiccups" do
        allow(conn).to receive(:get).and_raise(RuntimeError, "boom")
        expect { leader.release }.not_to raise_error
      end
    end

    describe "TTL defaults" do
      it "defaults to 3× the publish interval" do
        leader = described_class.new(key: "k", interval: 30)
        expect(leader.ttl).to eq(90)
      end

      it "accepts a custom ttl" do
        leader = described_class.new(key: "k", interval: 30, ttl: 200)
        expect(leader.ttl).to eq(200)
      end
    end

    describe "instance id" do
      it "is unique across processes" do
        id1 = described_class.new(key: "k", interval: 60).id
        id2 = described_class.new(key: "k", interval: 60).id
        expect(id1).not_to eq(id2)
        expect(id1).to include(Socket.gethostname)
      end
    end
  end
end

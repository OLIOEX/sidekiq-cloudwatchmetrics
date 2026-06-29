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
        publisher = instance_double(Sidekiq::CloudWatchMetrics::Publisher, records_execution_times?: false)
        expect(Sidekiq::CloudWatchMetrics::Publisher).to receive(:new).and_return(publisher)

        Sidekiq::CloudWatchMetrics.enable!

        # Look, this is hard.
        expect(sidekiq_options[:lifecycle_events][:startup]).not_to be_empty
        expect(sidekiq_options[:lifecycle_events][:quiet]).not_to be_empty
        expect(sidekiq_options[:lifecycle_events][:shutdown]).not_to be_empty
      end

      context "execution-time recorder middleware", if: Sidekiq.respond_to?(:default_configuration) do
        let(:chain) { Sidekiq.default_configuration.server_middleware }

        before { allow(Aws::CloudWatch::Client).to receive(:new).and_return(instance_double(Aws::CloudWatch::Client)) }
        after { chain.remove(Sidekiq::CloudWatchMetrics::ExecutionRecorder) }

        it "is installed when execution-time metrics are enabled" do
          Sidekiq::CloudWatchMetrics.enable!(metrics: %i[job_execution_time_p99])
          expect(chain.exists?(Sidekiq::CloudWatchMetrics::ExecutionRecorder)).to be(true)
        end

        it "is not installed when no execution-time metrics are enabled" do
          Sidekiq::CloudWatchMetrics.enable!(metrics: %i[processed_jobs])
          expect(chain.exists?(Sidekiq::CloudWatchMetrics::ExecutionRecorder)).to be(false)
        end

        it "is installed only once across repeated enable! calls" do
          Sidekiq::CloudWatchMetrics.enable!(metrics: %i[job_execution_time_p99])
          Sidekiq::CloudWatchMetrics.enable!(metrics: %i[job_execution_time_p99])
          installed = chain.entries.count { |e| e.klass == Sidekiq::CloudWatchMetrics::ExecutionRecorder }
          expect(installed).to eq(1)
        end
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
        # By default drain no execution-time samples so existing tests don't
        # need to know about that code path. The dedicated context below
        # overrides this to exercise the percentile calculation.
        allow_any_instance_of(Sidekiq::CloudWatchMetrics::ExecutionSamples)
          .to receive(:drain).and_return({})
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

        context "selecting job execution time percentiles" do
          subject(:publisher) do
            Sidekiq::CloudWatchMetrics::Publisher.new(
              client: client,
              metrics: %i[job_execution_time_p50 job_execution_time_p95 job_execution_time_p99],
              execution_samples: execution_samples,
            )
          end

          let(:execution_samples) { instance_double(Sidekiq::CloudWatchMetrics::ExecutionSamples) }

          # Raw per-class execution durations (milliseconds) for the drained
          # minute. SlowJob's tail runs far past the old 335s histogram ceiling.
          #   FastJob: 100 × 12ms          → every percentile 0.012s
          #   SlowJob: 90 × 50ms, 8 × 1.1s, 1 × 600s, 1 × 900s (n = 100)
          #     p50 → sorted[49]  = 50ms     = 0.05s
          #     p95 → sorted[94]  = 1100ms   = 1.1s
          #     p99 → sorted[98]  = 600000ms = 600.0s  (no ceiling!)
          let(:drained_samples) do
            {
              "FastJob" => Array.new(100, 12.0),
              "SlowJob" => ([50.0] * 90) + ([1_100.0] * 8) + [600_000.0, 900_000.0],
            }
          end

          before { allow(execution_samples).to receive(:drain).and_return(drained_samples) }

          it "publishes per-class p50/p95/p99 in true seconds, with no 335s ceiling" do
            Timecop.freeze(now = Time.now) do
              publisher.publish

              expect(client).to have_received(:put_metric_data).with(
                namespace: "Sidekiq",
                metric_data: contain_exactly(
                  {metric_name: "JobExecutionTimeP50", dimensions: [{name: "JobClass", value: "FastJob"}], timestamp: now, value: 0.012, unit: "Seconds"},
                  {metric_name: "JobExecutionTimeP95", dimensions: [{name: "JobClass", value: "FastJob"}], timestamp: now, value: 0.012, unit: "Seconds"},
                  {metric_name: "JobExecutionTimeP99", dimensions: [{name: "JobClass", value: "FastJob"}], timestamp: now, value: 0.012, unit: "Seconds"},
                  {metric_name: "JobExecutionTimeP50", dimensions: [{name: "JobClass", value: "SlowJob"}], timestamp: now, value: 0.05, unit: "Seconds"},
                  {metric_name: "JobExecutionTimeP95", dimensions: [{name: "JobClass", value: "SlowJob"}], timestamp: now, value: 1.1, unit: "Seconds"},
                  {metric_name: "JobExecutionTimeP99", dimensions: [{name: "JobClass", value: "SlowJob"}], timestamp: now, value: 600.0, unit: "Seconds"},
                ),
              )
            end
          end

          it "drains the minute that finished one window ago" do
            Timecop.freeze(now = Time.now) do
              expect(execution_samples).to receive(:drain)
                .with(now - Sidekiq::CloudWatchMetrics::Publisher::EXECUTION_WINDOW_SECONDS)
                .and_return({})
              publisher.publish
            end
          end

          it "does not call Sidekiq::Stats, ProcessSet, or Queue" do
            expect(Sidekiq::Stats).not_to receive(:new)
            expect(Sidekiq::ProcessSet).not_to receive(:new)
            expect(Sidekiq::Queue).not_to receive(:new)
            publisher.publish
          end

          context "with a class that has no recorded activity" do
            let(:drained_samples) { {"FastJob" => Array.new(100, 12.0), "SlowJob" => []} }

            it "publishes nothing for that class" do
              Timecop.freeze(now = Time.now) do
                publisher.publish

                expect(client).to have_received(:put_metric_data).with(
                  namespace: "Sidekiq",
                  metric_data: contain_exactly(
                    {metric_name: "JobExecutionTimeP50", dimensions: [{name: "JobClass", value: "FastJob"}], timestamp: now, value: 0.012, unit: "Seconds"},
                    {metric_name: "JobExecutionTimeP95", dimensions: [{name: "JobClass", value: "FastJob"}], timestamp: now, value: 0.012, unit: "Seconds"},
                    {metric_name: "JobExecutionTimeP99", dimensions: [{name: "JobClass", value: "FastJob"}], timestamp: now, value: 0.012, unit: "Seconds"},
                  ),
                )
              end
            end
          end
        end

        context "selecting a single execution-time percentile" do
          subject(:publisher) do
            Sidekiq::CloudWatchMetrics::Publisher.new(
              client: client,
              metrics: [:job_execution_time_p99],
              execution_samples: execution_samples,
            )
          end

          let(:execution_samples) { instance_double(Sidekiq::CloudWatchMetrics::ExecutionSamples) }

          # n = 100: p99 → sorted[98] = 5000ms = 5.0s
          before do
            allow(execution_samples).to receive(:drain).and_return(
              "SlowJob" => ([100.0] * 90) + ([5_000.0] * 10),
            )
          end

          it "drains samples once and publishes only the enabled percentile" do
            Timecop.freeze(now = Time.now) do
              publisher.publish

              expect(client).to have_received(:put_metric_data).with(
                namespace: "Sidekiq",
                metric_data: [
                  {metric_name: "JobExecutionTimeP99", dimensions: [{name: "JobClass", value: "SlowJob"}], timestamp: now, value: 5.0, unit: "Seconds"},
                ],
              )
            end
          end
        end

        context "when no execution-time metrics are enabled" do
          subject(:publisher) do
            Sidekiq::CloudWatchMetrics::Publisher.new(client: client, metrics: [:processed_jobs])
          end

          it "does not drain samples or publish execution-time metrics" do
            Timecop.freeze(now = Time.now) do
              publisher.publish

              expect(client).to have_received(:put_metric_data).with(
                namespace: "Sidekiq",
                metric_data: [{metric_name: "ProcessedJobs", timestamp: now, value: 123, unit: "Count"}],
              )
            end
          end
        end

        context "with min_job_seconds set to a non-positive value" do
          it "treats it as nil (filter disabled)" do
            publisher = Sidekiq::CloudWatchMetrics::Publisher.new(client: client, min_job_seconds: 0)
            expect(publisher.instance_variable_get(:@min_job_seconds)).to be_nil
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

  describe Sidekiq::CloudWatchMetrics::ExecutionRecorder do
    let(:samples) { instance_double(Sidekiq::CloudWatchMetrics::ExecutionSamples) }

    before do
      allow(Sidekiq::CloudWatchMetrics::ExecutionSamples).to receive(:new).and_return(samples)
      allow(samples).to receive(:record)
    end

    # Drive the monotonic clock so each job has a deterministic duration: the
    # middleware reads the clock once before yielding and once after.
    def run(recorder, job, &block)
      block ||= proc {}
      recorder.call(double(:worker), job, "default", &block)
    end

    it "records the duration (ms) for jobs at or above the threshold" do
      allow(Process).to receive(:clock_gettime).and_return(100.0, 100.5) # 0.5s
      recorder = described_class.new("sidekiq-test", 0.5)

      run(recorder, {"class" => "SlowJob"})

      expect(samples).to have_received(:record).with("SlowJob", a_value_within(0.001).of(500.0))
    end

    it "skips jobs faster than the threshold" do
      allow(Process).to receive(:clock_gettime).and_return(100.0, 100.1) # ~0.1s
      recorder = described_class.new("sidekiq-test", 0.5)

      run(recorder, {"class" => "FastJob"})

      expect(samples).not_to have_received(:record)
    end

    it "records every job when the threshold is nil" do
      allow(Process).to receive(:clock_gettime).and_return(100.0, 100.001) # ~1ms
      recorder = described_class.new("sidekiq-test", nil)

      run(recorder, {"class" => "FastJob"})

      expect(samples).to have_received(:record).with("FastJob", a_value_within(0.5).of(1.0))
    end

    it "prefers the wrapped (ActiveJob) class name" do
      allow(Process).to receive(:clock_gettime).and_return(100.0, 101.0)
      recorder = described_class.new("sidekiq-test", 0.5)

      run(recorder, {"class" => "Sidekiq::ActiveJob::Wrapper", "wrapped" => "MyMailerJob"})

      expect(samples).to have_received(:record).with("MyMailerJob", anything)
    end

    it "still records when the job raises, then re-raises" do
      allow(Process).to receive(:clock_gettime).and_return(100.0, 100.6)
      recorder = described_class.new("sidekiq-test", 0.5)

      expect {
        run(recorder, {"class" => "BoomJob"}) { raise "boom" }
      }.to raise_error("boom")

      expect(samples).to have_received(:record).with("BoomJob", anything)
    end
  end

  describe Sidekiq::CloudWatchMetrics::ExecutionSamples do
    subject(:samples) { described_class.new(namespace: "sidekiq-test", sample_cap: 1000) }

    let(:conn) { double(:redis_conn) }
    let(:time) { Time.utc(2026, 6, 29, 12, 34, 56) }
    let(:window) { "20260629T1234" }
    let(:prefix) { "sidekiq-cloudwatchmetrics:exectimes:sidekiq-test" }

    before { allow(Sidekiq).to receive(:redis).and_yield(conn) }

    describe "#record" do
      it "appends the duration, caps the list, indexes the class, and sets TTLs" do
        list = "#{prefix}:#{window}:samples:MyJob"
        index = "#{prefix}:#{window}:classes"

        expect(conn).to receive(:rpush).with(list, 512.5)
        expect(conn).to receive(:ltrim).with(list, -1000, -1)
        expect(conn).to receive(:expire).with(list, 180)
        expect(conn).to receive(:sadd).with(index, "MyJob")
        expect(conn).to receive(:expire).with(index, 180)

        samples.record("MyJob", 512.5, time)
      end

      it "swallows Redis errors so the job is never disturbed" do
        allow(conn).to receive(:rpush).and_raise(RuntimeError, "redis down")
        expect { samples.record("MyJob", 1.0, time) }.not_to raise_error
      end
    end

    describe "#drain" do
      let(:index) { "#{prefix}:#{window}:classes" }
      let(:list_a) { "#{prefix}:#{window}:samples:JobA" }
      let(:list_b) { "#{prefix}:#{window}:samples:JobB" }

      it "reads each class's samples as floats and deletes the drained keys" do
        allow(conn).to receive(:smembers).with(index).and_return(["JobA", "JobB"])
        allow(conn).to receive(:lrange).with(list_a, 0, -1).and_return(["10.0", "20.5"])
        allow(conn).to receive(:lrange).with(list_b, 0, -1).and_return(["1000.0"])
        allow(conn).to receive(:del)

        result = samples.drain(time)

        expect(result).to eq("JobA" => [10.0, 20.5], "JobB" => [1000.0])
        expect(conn).to have_received(:del).with(list_a)
        expect(conn).to have_received(:del).with(list_b)
        expect(conn).to have_received(:del).with(index)
      end

      it "returns an empty hash when the minute has no recorded classes" do
        allow(conn).to receive(:smembers).with(index).and_return([])

        expect(samples.drain(time)).to eq({})
      end
    end
  end
end

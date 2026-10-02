require 'temporal/metric_keys'

# This class implements a very simple ThreadPool with the ability to
# block until at least one thread becomes available. This allows Pollers
# to only poll when there's an available thread in the pool.
#
# NOTE: There's a minor race condition that can occur between calling
#       #wait_for_available_threads and #schedule, but should be rare
#
module Temporal
  class ThreadPool
    DEFAULT_METRICS_REPORT_INTERVAL_SECONDS = 10

    attr_reader :size

    def initialize(
      size,
      config,
      metrics_tags,
      worker_metrics_tags: nil,
      metrics_report_interval_seconds: DEFAULT_METRICS_REPORT_INTERVAL_SECONDS
    )
      @size = size
      @metrics_tags = metrics_tags
      @worker_metrics_tags = worker_metrics_tags
      @metrics_report_interval_seconds = metrics_report_interval_seconds
      @queue = Queue.new
      @mutex = Mutex.new
      @config = config
      @availability = ConditionVariable.new
      @available_threads = size
      @pool = Array.new(size) do |_i|
        Thread.new { poll }
      end

      # Task transitions can be shorter than a metrics aggregation interval. A
      # periodic snapshot makes this a stable saturation signal and reports idle
      # workers even when they have not received a task recently.
      start_metrics_reporter if worker_metrics_tags
    end

    def report_metrics
      Temporal.metrics.gauge(Temporal::MetricKeys::THREAD_POOL_AVAILABLE_THREADS, @available_threads, @metrics_tags)
    end

    def wait_for_available_threads
      @mutex.synchronize do
        @availability.wait(@mutex) while @available_threads <= 0
      end
    end

    def schedule(&block)
      @mutex.synchronize do
        @available_threads -= 1
        @queue << block
      end

      report_metrics
    end

    def shutdown
      stop_metrics_reporter

      size.times do
        schedule { throw EXIT_SYMBOL }
      end

      @pool.each(&:join)
    end

    private

    EXIT_SYMBOL = :exit

    def report_worker_slot_metrics
      available_threads = @mutex.synchronize { @available_threads }

      Temporal.metrics.gauge(
        Temporal::MetricKeys::WORKER_TASK_SLOTS_AVAILABLE,
        available_threads,
        @worker_metrics_tags
      )
      Temporal.metrics.gauge(
        Temporal::MetricKeys::WORKER_TASK_SLOTS_USED,
        size - available_threads,
        @worker_metrics_tags
      )
    end

    def start_metrics_reporter
      @metrics_reporter_mutex = Mutex.new
      @metrics_reporter_condition = ConditionVariable.new
      @metrics_reporter_shutdown = false

      report_worker_slot_metrics
      @metrics_reporter_thread = Thread.new do
        loop do
          shutting_down = @metrics_reporter_mutex.synchronize do
            @metrics_reporter_condition.wait(@metrics_reporter_mutex, @metrics_report_interval_seconds)
            @metrics_reporter_shutdown
          end
          break if shutting_down

          report_worker_slot_metrics
        end
      end
    end

    def stop_metrics_reporter
      return unless @metrics_reporter_thread

      @metrics_reporter_mutex.synchronize do
        @metrics_reporter_shutdown = true
        @metrics_reporter_condition.signal
      end
      @metrics_reporter_thread.join
    end

    def poll
      Thread.current.abort_on_exception = true

      catch(EXIT_SYMBOL) do
        loop do
          job = @queue.pop
          begin
            job.call
          rescue StandardError => e
            Temporal.logger.error('Error reached top of thread pool thread', { error: e.inspect })
            Temporal::ErrorHandler.handle(e, @config)
          rescue Exception => ex
            Temporal.logger.error('Exception reached top of thread pool thread', { error: ex.inspect })
            Temporal::ErrorHandler.handle(ex, @config)
            raise
          end
          @mutex.synchronize do
            @available_threads += 1
            @availability.signal
          end

          report_metrics
        end
      end
    end
  end
end

require 'temporal/thread_pool'
require 'timeout'

describe Temporal::ThreadPool do
  before do
    allow(Temporal.metrics).to receive(:gauge)
  end

  let(:config) { Temporal::Configuration.new }
  let(:size) { 2 }
  let(:tags) { { foo: 'bar', bat: 'baz' } }
  let(:thread_pool) { described_class.new(size, config, tags) }
  let(:worker_metrics_tags) do
    {
      namespace: 'test-namespace',
      task_queue: 'test-task-queue',
      worker_type: 'ActivityWorker',
      worker_id: 'worker-1'
    }
  end

  describe '#new' do
    it 'executes one task on a thread and exits' do
      times = 0

      thread_pool.schedule do
        times += 1
      end

      thread_pool.shutdown

      expect(times).to eq(1)
    end

    it 'handles error without exiting' do
      times = 0

      thread_pool.schedule do
        times += 1
        raise 'failure'
      end

      thread_pool.shutdown

      expect(times).to eq(1)
    end

    it 'handles exception with exiting' do
      Thread.report_on_exception = false
      times = 0

      thread_pool.schedule do
        times += 1
        raise Exception, 'crash'
      end

      begin
        thread_pool.shutdown
      rescue Exception => e
        'ok'
      end

      expect(times).to eq(1)
    end

    it 'reports thread available metrics' do
      thread_pool.schedule do
      end

      thread_pool.shutdown

      # Thread behavior is not deterministic. Ensure the calls match without
      # verifying exact gauge values.
      expect(Temporal.metrics)
        .to have_received(:gauge)
        .with(
          Temporal::MetricKeys::THREAD_POOL_AVAILABLE_THREADS,
          instance_of(Integer),
          tags
        )
        .at_least(:once)
    end

    it 'periodically reports available and used task slots for an individual worker' do
      pool = described_class.new(
        size,
        config,
        tags,
        worker_metrics_tags: worker_metrics_tags,
        metrics_report_interval_seconds: 0.01
      )

      sleep 0.02
      pool.shutdown

      expect(Temporal.metrics)
        .to have_received(:gauge)
        .with(Temporal::MetricKeys::WORKER_TASK_SLOTS_AVAILABLE, size, worker_metrics_tags)
        .at_least(:twice)
      expect(Temporal.metrics)
        .to have_received(:gauge)
        .with(Temporal::MetricKeys::WORKER_TASK_SLOTS_USED, 0, worker_metrics_tags)
        .at_least(:twice)
    end

    it 'shuts down promptly when the metrics thread has not started yet' do
      start_threads = Queue.new
      allow(Thread).to receive(:new).and_wrap_original do |original, &block|
        original.call do
          start_threads.pop
          block.call
        end
      end
      pool = described_class.new(size, config, tags, worker_metrics_tags: worker_metrics_tags)
      allow(Thread).to receive(:new).and_call_original

      # Wait until shutdown is joining the reporter before allowing that
      # reporter to run. Other threads, including Timeout's watchdog, run normally.
      shutdown_thread = Thread.start { pool.shutdown }
      Timeout.timeout(2) { Thread.pass until shutdown_thread.status == 'sleep' }
      (size + 1).times { start_threads << true }

      expect(shutdown_thread.join(1)).to eq(shutdown_thread)
    ensure
      (size + 1).times { start_threads << true }
      shutdown_thread&.join
    end

    it 'reports task slot usage while a task is running' do
      task_started = Queue.new
      finish_task = Queue.new
      pool = described_class.new(
        size,
        config,
        tags,
        worker_metrics_tags: worker_metrics_tags,
        metrics_report_interval_seconds: 0.01
      )

      pool.schedule do
        task_started << true
        finish_task.pop
      end
      task_started.pop
      sleep 0.02
      finish_task << true
      pool.shutdown

      expect(Temporal.metrics)
        .to have_received(:gauge)
        .with(Temporal::MetricKeys::WORKER_TASK_SLOTS_AVAILABLE, size - 1, worker_metrics_tags)
        .at_least(:once)
      expect(Temporal.metrics)
        .to have_received(:gauge)
        .with(Temporal::MetricKeys::WORKER_TASK_SLOTS_USED, 1, worker_metrics_tags)
        .at_least(:once)
    end
  end
end

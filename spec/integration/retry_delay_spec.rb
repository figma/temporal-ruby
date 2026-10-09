require 'temporal/worker'
require 'tempfile'

class RetryDelayIntegrationError < StandardError
  include Temporal::RetryDelay
end

class RetryDelayIntegrationActivity < Temporal::Activity
  retry_policy interval: 1, backoff: 1, max_interval: 1, max_attempts: 2

  def execute(path)
    File.open(path, 'a') { |file| file.puts(Process.clock_gettime(Process::CLOCK_REALTIME)) }
    error = RetryDelayIntegrationError.new('rate limited')
    error.next_retry_delay = 2.3
    raise error
  end
end

class RetryDelayIntegrationWorkflow < Temporal::Workflow
  def execute(path)
    RetryDelayIntegrationActivity.execute!(path)
  rescue RetryDelayIntegrationError => error
    [error.class.name, error.message, error.next_retry_delay]
  end
end

describe 'Activity next_retry_delay against Temporal Server' do
  it 'uses the failure delay instead of the retry policy interval' do
    skip 'Set TEMPORAL_TEST_PORT to run against a local Temporal test server' unless ENV['TEMPORAL_TEST_PORT']

    config = Temporal::Configuration.new
    config.host = '127.0.0.1'
    config.port = ENV.fetch('TEMPORAL_TEST_PORT').to_i
    config.namespace = 'default'
    config.task_queue = "retry-delay-#{SecureRandom.uuid}"
    config.logger.level = Logger::FATAL
    client = Temporal::Client.new(config)

    Tempfile.create('temporal-retry-delay-') do |attempt_file|
      worker_pid = fork do
        worker = Temporal::Worker.new(config)
        worker.register_workflow(RetryDelayIntegrationWorkflow)
        worker.register_activity(RetryDelayIntegrationActivity)
        worker.start
      end
      begin
        workflow_id = SecureRandom.uuid
        run_id = client.start_workflow(RetryDelayIntegrationWorkflow, attempt_file.path,
                                       options: { workflow_id: workflow_id })
        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 10
        pending_failure = nil
        until pending_failure || Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
          response = client.connection.describe_workflow_execution(namespace: 'default', workflow_id: workflow_id,
                                                                   run_id: run_id)
          pending_failure = response.pending_activities.first&.last_failure
          sleep 0.05 unless pending_failure
        end
        expect(pending_failure).not_to be_nil
        expect(pending_failure.application_failure_info.next_retry_delay.seconds).to eq(2)
        expect(pending_failure.application_failure_info.next_retry_delay.nanos).to eq(300_000_000)

        expect(client.await_workflow_result(RetryDelayIntegrationWorkflow, workflow_id: workflow_id,
                                            run_id: run_id, timeout: 10))
          .to eq(['RetryDelayIntegrationError', 'rate limited', 2.3])
        attempts = File.readlines(attempt_file.path).map(&:to_f)
        expect(attempts.size).to eq(2)
        expect(attempts.last - attempts.first).to be >= 2.0
      ensure
        begin
          Process.kill('TERM', worker_pid)
        rescue Errno::ESRCH
          # Preserve the original test failure if the worker already exited.
        end
        Process.wait(worker_pid)
      end
    end
  end
end

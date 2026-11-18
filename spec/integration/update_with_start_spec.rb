# Run against an isolated local Temporal 1.32 server:
# TEMPORAL_UPDATE_INTEGRATION=1 TEMPORAL_UPDATE_PORT=17233 bundle exec rspec spec/integration/update_with_start_spec.rb
require 'temporal/worker'
require 'temporal/testing/replay_tester'

RSpec.describe 'Update-With-Start against Temporal', if: ENV['TEMPORAL_UPDATE_INTEGRATION'] == '1' do
  class UpdateIntegrationActivity < Temporal::Activity
    def execute(value)
      raise ArgumentError, 'activity failure' if value == 'activity-failure'
      "activity:#{value}"
    end
  end

  class UpdateIntegrationWorkflow < Temporal::Workflow
    def execute(account)
      @values = []
      @busy = false
      workflow.on_signal('stop') { @stop = true }
      workflow.on_signal('append') { |value| @values << value }
      workflow.on_query('values') { @values }
      workflow.on_query('callback_status') { { 'info' => @callback_info, 'unwound' => @callback_unwound } }
      workflow.on_update('callback_guard') do
        future = UpdateIntegrationActivity.execute('activity-failure', options: { retry_policy: { max_attempts: 1 } })
        future.failed do
          @callback_info = workflow.current_update
          workflow.continue_as_new
        end
        begin
          workflow.wait_until { false }
        ensure
          @callback_unwound = true
        end
      end
      workflow.on_update('append', validator: ->(value) { raise ArgumentError, 'invalid value' if value == 'invalid' }) do |value|
        raise ArgumentError, 'handler failure' if value == 'fail'
        workflow.wait_until { !@busy }
        @busy = true
        begin
          if value == 'block'
            workflow.wait_until { @released }
          end
          @values << UpdateIntegrationActivity.execute!(value)
          { 'account' => account, 'values' => @values.dup, 'update_id' => workflow.current_update[:id] }
        ensure
          @busy = false
        end
      end
      workflow.on_update('release') { @released = true }
      workflow.wait_until { @stop }
      workflow.wait_until { workflow.all_update_handlers_finished? }
      @values
    end
  end

  class UpdateAutoCompletionIntegrationWorkflow < Temporal::Workflow
    def execute
      workflow.on_query('main_returned') { @main_returned }
      workflow.on_signal('release') { @released = true }
      workflow.on_update('block') do
        @received = true
        workflow.wait_until { @released }
        'update-result'
      end
      workflow.wait_until { @received }
      @main_returned = true
      # Deliberately no all_update_handlers_finished? wait: the SDK must defer
      # this result across workflow tasks while the accepted Update is blocked.
      'workflow-result'
    end
  end

  class UpdateValidatorFutureIntegrationWorkflow < Temporal::Workflow
    def execute(activity_value)
      future = UpdateIntegrationActivity.execute(activity_value, options: { retry_policy: { max_attempts: 1 } })
      workflow.on_signal('stop') { @stop = true }
      workflow.on_query('callback_ran') { @callback_ran == true }
      workflow.on_update('register_callback', validator: lambda { |callback_type, expected_state|
        if future.finished? != (expected_state == 'finished')
          raise ArgumentError, 'unexpected Future state in test validator'
        end
        future.public_send(callback_type) do
          @callback_ran = true
          workflow.complete('must not close the workflow')
        end
      }) { 'must not accept the update' }
      workflow.on_update('wait_future') do
        future.wait
        future.failed? ? 'failed' : 'ready'
      end
      workflow.wait_until { @stop }
      'workflow-result'
    end
  end

  class UpdateIntegrationWorker < Temporal::Worker
    # Test thread must not install process-wide signal handlers.
    def trap_signals; end
  end

  before(:all) do
    @config = Temporal::Configuration.new
    @config.host = '127.0.0.1'
    @config.port = Integer(ENV.fetch('TEMPORAL_UPDATE_PORT', '17233'))
    @config.namespace = 'default'
    @config.task_queue = "ruby-update-integration-#{SecureRandom.uuid}"
    @config.timeouts = { execution: 120, run: 120, task: 10, start_to_close: 15 }
    @client = Temporal::Client.new(@config)
    version = @client.connection.get_system_info.server_version
    raise "Expected Temporal1.32, got #{version}" unless version.start_with?('1.32.')
    puts "Integration server=#{version} ruby=#{RUBY_VERSION} task_queue=#{@config.task_queue}"
    start_worker
  end

  after(:all) { stop_worker }

  before { @workflow_id = "ruby-update-#{SecureRandom.uuid}" }

  after do
    @client.terminate_workflow(@workflow_id, reason: 'integration cleanup') rescue nil
  end

  def start_worker
    raise 'Previous worker was not stopped' if @worker || @worker_thread
    @worker = UpdateIntegrationWorker.new(@config)
    @worker.register_workflow(UpdateIntegrationWorkflow)
    @worker.register_workflow(UpdateAutoCompletionIntegrationWorkflow)
    @worker.register_workflow(UpdateValidatorFutureIntegrationWorkflow)
    @worker.register_activity(UpdateIntegrationActivity)
    @worker_thread = Thread.new { @worker.start }
  end

  def stop_worker
    @worker&.stop
    if @worker_thread
      raise 'Worker thread did not terminate before restart' unless @worker_thread.join(10)
      raise 'Worker thread is still alive after stop' if @worker_thread.alive?
    end
    @worker = @worker_thread = nil
  end

  def with_start(value, id: SecureRandom.uuid, policy: :use_existing, wait: :completed, reuse: :allow)
    @client.start_update_with_start_workflow(UpdateIntegrationWorkflow, 'append', value,
      start_args: ['account-1'], update_id: id, options: { workflow_id: @workflow_id, workflow_id_reuse_policy: reuse },
      workflow_id_conflict_policy: policy, wait_for_stage: wait, timeout: 20)
  end

  it 'starts when absent, updates the existing run, returns Activity results and replays its completed history' do
    first = with_start('one')
    expect(first.workflow_started).to eq(true)
    expect(first.result['values']).to eq(['activity:one'])
    second = with_start('two')
    expect(second.run_id).to eq(first.run_id)
    expect(second.workflow_started).to eq(false)
    expect(second.result['values']).to eq(['activity:one', 'activity:two'])
    @client.signal_workflow(nil, 'stop', @workflow_id, first.run_id)
    expect(@client.await_workflow_result(nil, workflow_id: @workflow_id, run_id: first.run_id, timeout: 20)).to eq(['activity:one', 'activity:two'])
    history = @client.get_workflow_history(workflow_id: @workflow_id, run_id: first.run_id)
    Temporal::Testing::ReplayTester.new(config: @config).replay_history(UpdateIntegrationWorkflow, history)
  end

  it 'rejects invalid updates, reports handler failures, and keeps the workflow usable' do
    expect { with_start('invalid').result }.to raise_error(Temporal::WorkflowUpdateFailed, /invalid value/)
    expect { with_start('fail').result }.to raise_error(Temporal::WorkflowUpdateFailed, /handler failure/)
    expect(with_start('good').result['values']).to eq(['activity:good'])
  end

  it 'rejects malformed Update headers before acceptance on repeated requests and leaves no active-handler leak' do
    first = with_start('one')
    # Deliberately bypass client serialization to send an unsupported payload
    # encoding, as another client or a mismatched codec could do.
    request = Temporalio::Api::WorkflowService::V1::UpdateWorkflowExecutionRequest.new(
      namespace: @config.namespace,
      workflow_execution: Temporalio::Api::Common::V1::WorkflowExecution.new(workflow_id: @workflow_id, run_id: first.run_id),
      wait_policy: Temporalio::Api::Update::V1::WaitPolicy.new(lifecycle_stage: 3),
      request: Temporalio::Api::Update::V1::Request.new(
        meta: Temporalio::Api::Update::V1::Meta.new(update_id: 'malformed-header'),
        input: Temporalio::Api::Update::V1::Input.new(name: 'append', args: @config.converter.to_payloads(['must-not-append']),
          header: Temporalio::Api::Common::V1::Header.new(fields: {
            'bad' => Temporalio::Api::Common::V1::Payload.new(metadata: { 'encoding' => 'unsupported-header-test' }, data: 'invalid')
          }))
      )
    )
    2.times do
      response = @client.connection.send(:client).update_workflow_execution(request, deadline: Time.now + 20)
      expect(response.outcome.failure.application_failure_info.type).to include('ConverterNotFound')
    end
    expect(with_start('after-header').result['values']).to eq(['activity:one', 'activity:after-header'])
    @client.signal_workflow(nil, 'stop', @workflow_id, first.run_id)
    expect(@client.await_workflow_result(nil, workflow_id: @workflow_id, run_id: first.run_id, timeout: 20)).to eq(
      ['activity:one', 'activity:after-header'])
    history = @client.get_workflow_history(workflow_id: @workflow_id, run_id: first.run_id)
    accepted = history.events.select { |event| event.type == 'WORKFLOW_EXECUTION_UPDATE_ACCEPTED' }
    expect(accepted.map { |event| event.attributes.protocol_instance_id }).not_to include('malformed-header')
    expect(history.events.map(&:type)).not_to include('WORKFLOW_TASK_FAILED')
    Temporal::Testing::ReplayTester.new(config: @config).replay_history(UpdateIntegrationWorkflow, history)
  end

  it 'rejects validator callbacks on pending and failed Activities, then replays cleanly' do
    handle = @client.start_update_with_start_workflow(UpdateValidatorFutureIntegrationWorkflow, 'register_callback',
      'failed', 'pending', start_args: ['activity-failure'], options: { workflow_id: @workflow_id },
      workflow_id_conflict_policy: :use_existing, update_id: 'pending-validator', timeout: 20)
    expect { handle.result(timeout: 20) }.to raise_error(Temporal::WorkflowUpdateFailed, /must not register Future callbacks/)
    expect(@client.execute_workflow_update(nil, 'wait_future', @workflow_id, handle.run_id, timeout: 20)).to eq('failed')
    expect do
      @client.execute_workflow_update(nil, 'register_callback', @workflow_id, handle.run_id, 'failed', 'finished',
        update_id: 'finished-validator', timeout: 20)
    end.to raise_error(Temporal::WorkflowUpdateFailed, /must not register Future callbacks/)
    expect(@client.query_workflow(nil, 'callback_ran', @workflow_id, handle.run_id)).to eq(false)
    @client.signal_workflow(nil, 'stop', @workflow_id, handle.run_id)
    expect(@client.await_workflow_result(nil, workflow_id: @workflow_id, run_id: handle.run_id, timeout: 20)).to eq('workflow-result')
    history = @client.get_workflow_history(workflow_id: @workflow_id, run_id: handle.run_id)
    accepted = history.events.select { |event| event.type == 'WORKFLOW_EXECUTION_UPDATE_ACCEPTED' }
    expect(accepted.map { |event| event.attributes.protocol_instance_id }).not_to include('pending-validator', 'finished-validator')
    Temporal::Testing::ReplayTester.new(config: @config).replay_history(UpdateValidatorFutureIntegrationWorkflow, history)
  end

  it 'retains ownership in a failed Activity callback, rejects continuing as new, and replays the failure' do
    first = with_start('one')
    update_id = 'failed-continue-as-new'
    expect do
      @client.execute_workflow_update(nil, 'callback_guard', @workflow_id, first.run_id,
        update_id: update_id, timeout: 20)
    end.to raise_error(Temporal::WorkflowUpdateFailed, /Update handlers cannot/)
    status = @client.query_workflow(nil, 'callback_status', @workflow_id, first.run_id)
    expect(status['info'][:id]).to eq(update_id)
    expect(status['info'][:name]).to eq('callback_guard')
    expect(status['unwound']).to eq(true)
    expect(with_start('after-callback').result['values']).to eq(['activity:one', 'activity:after-callback'])
    @client.signal_workflow(nil, 'stop', @workflow_id, first.run_id)
    @client.await_workflow_result(nil, workflow_id: @workflow_id, run_id: first.run_id, timeout: 20)
    history = @client.get_workflow_history(workflow_id: @workflow_id, run_id: first.run_id)
    Temporal::Testing::ReplayTester.new(config: @config).replay_history(UpdateIntegrationWorkflow, history)
  end

  it 'automatically defers workflow completion across tasks until a blocked accepted Update finishes, then replays both outcomes' do
    handle = @client.start_update_with_start_workflow(UpdateAutoCompletionIntegrationWorkflow, 'block',
      options: { workflow_id: @workflow_id }, workflow_id_conflict_policy: :use_existing,
      wait_for_stage: :accepted, timeout: 20)
    expect(@client.query_workflow(nil, 'main_returned', @workflow_id, handle.run_id)).to eq(true)
    blocked_history = @client.get_workflow_history(workflow_id: @workflow_id, run_id: handle.run_id)
    expect(blocked_history.events.map(&:type)).to include('WORKFLOW_EXECUTION_UPDATE_ACCEPTED')
    expect(blocked_history.events.map(&:type)).not_to include('WORKFLOW_EXECUTION_UPDATE_COMPLETED', 'WORKFLOW_EXECUTION_COMPLETED')
    @client.signal_workflow(nil, 'release', @workflow_id, handle.run_id)
    expect(handle.result(timeout: 20)).to eq('update-result')
    expect(@client.await_workflow_result(nil, workflow_id: @workflow_id, run_id: handle.run_id, timeout: 20)).to eq('workflow-result')
    history = @client.get_workflow_history(workflow_id: @workflow_id, run_id: handle.run_id)
    types = history.events.map(&:type)
    expect(types.index('WORKFLOW_EXECUTION_UPDATE_COMPLETED')).to be < types.index('WORKFLOW_EXECUTION_COMPLETED')
    Temporal::Testing::ReplayTester.new(config: @config).replay_history(UpdateAutoCompletionIntegrationWorkflow, history)
  end

  it 'deduplicates concurrent requests for the same Update ID and returns the original result' do
    handles = 3.times.map { Thread.new { with_start('duplicate', id: 'same-update') } }.map(&:value)
    expect(handles.map(&:run_id).uniq.size).to eq(1)
    expect(handles.map { |handle| handle.result['values'] }.uniq).to eq([['activity:duplicate']])
    expect(with_start('different-input', id: 'same-update').result['values']).to eq(['activity:duplicate'])
  end

  it 'serializes concurrent handlers safely across Activity waits' do
    handles = 3.times.map { |i| Thread.new { with_start("concurrent-#{i}") } }.map(&:value)
    expect(handles.map { |h| h.result['values'].size }.sort).to eq([1, 2, 3])
  end

  it 'waits for acceptance, survives worker restart, and reattaches to the exact Update run' do
    handle = with_start('block', id: 'restart-update', wait: :accepted)
    stop_worker
    start_worker
    @client.execute_workflow_update(nil, 'release', @workflow_id, handle.run_id, timeout: 20)
    attached = @client.get_workflow_update_handle(@workflow_id, handle.run_id, handle.update_id)
    expect(attached.result(timeout: 20)['values']).to eq(['activity:block'])
  end

  it 'honors running conflict and completed reuse policies' do
    first = with_start('one')
    expect { with_start('conflict', policy: :fail) }.to raise_error(GRPC::AlreadyExists)
    @client.signal_workflow(nil, 'stop', @workflow_id, first.run_id)
    @client.await_workflow_result(nil, workflow_id: @workflow_id, run_id: first.run_id, timeout: 20)
    expect { with_start('closed-conflict', reuse: :reject) }.to raise_error(GRPC::AlreadyExists)
    next_run = with_start('new-run', reuse: :allow)
    expect(next_run.run_id).not_to eq(first.run_id)
    expect(next_run.result['values']).to eq(['activity:new-run'])
  end

  it 'replays Updates reapplied by a reset onto a new history branch' do
    first = with_start('before-reset')
    reset_run = @client.reset_workflow('default', @workflow_id, first.run_id, workflow_task_id: 4,
      reset_reapply_type: nil, reason: 'isolated Update replay integration')
    replayed = @client.get_workflow_update_handle(@workflow_id, reset_run, first.update_id)
    expect(replayed.result(timeout: 20)['values']).to eq(['activity:before-reset'])
    expect(@client.execute_workflow_update(nil, 'append', @workflow_id, reset_run, 'after-reset', timeout: 20)['values']).to eq(
      ['activity:before-reset', 'activity:after-reset'])
  end

  it 'preserves existing start and Signal-With-Start behavior' do
    run_id = @client.start_workflow(UpdateIntegrationWorkflow, 'account-1', options: {
      workflow_id: @workflow_id, signal_name: 'append', signal_input: 'signal-value'
    })
    result = @client.execute_workflow_update(nil, 'append', @workflow_id, run_id, 'updated', timeout: 20)
    expect(result['values']).to eq(['signal-value', 'activity:updated'])
  end
end

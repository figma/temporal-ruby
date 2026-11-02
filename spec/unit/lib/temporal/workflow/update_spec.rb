require 'temporal/workflow/executor'
require 'temporal/workflow/history'
require 'temporal/middleware/chain'

describe 'Workflow Updates' do
  let(:config) { Temporal::Configuration.new }
  let(:converter) { config.converter }
  let(:metadata) { Fabricate(:workflow_task_metadata) }
  let(:events) do
    [Fabricate(:api_workflow_execution_started_event, event_id: 1),
     Fabricate(:api_workflow_task_scheduled_event, event_id: 2),
     Fabricate(:api_workflow_task_started_event, event_id: 3)]
  end

  class UpdateTestWorkflow < Temporal::Workflow
    class << self
      attr_accessor :validations, :executions, :reject, :blocking_validator
    end

    def execute
      @values = []
      workflow.on_update('append', validator: lambda { |value|
        self.class.validations += 1
        workflow.sleep(1) if self.class.blocking_validator
        raise ArgumentError, 'rejected' if self.class.reject
      }) do |value|
        self.class.executions += 1
        @values << value
        @values.dup
      end
      workflow.on_update('release') { @released = true }
      workflow.on_update('blocked') do
        workflow.wait_until { @released }
        workflow.sleep(1)
        'after-timer'
      end
      workflow.on_update('finish') { @finished = true }
      workflow.wait_until { @finished }
      @values
    end
  end

  class UpdateExceptionTestWorkflow < Temporal::Workflow
    def execute
      workflow.on_update('script_validator', validator: ->(*) { raise NotImplementedError, 'validator not implemented' }) {}
      workflow.on_update('script_handler') { raise NotImplementedError, 'handler not implemented' }
      workflow.wait_until { false }
    end
  end

  class UpdateAutoCompletionWorkflow < Temporal::Workflow
    def execute
      workflow.on_update('delayed') do
        @received = true
        workflow.sleep(1)
        'update-result'
      end
      workflow.wait_until { @received }
      'workflow-result'
    end
  end

  class UpdateValidatorCallbackWorkflow < Temporal::Workflow
    class << self
      attr_accessor :futures, :callback_runs
    end

    def execute
      # No creator context: registration must check the active validator.
      self.class.futures = [:pending, :ready, :failed].to_h do |state|
        [state, Temporal::Workflow::Future.new(state, nil)]
      end
      self.class.futures[:ready].set('success')
      self.class.futures[:failed].fail(StandardError.new('failure'))
      self.class.callback_runs = 0
      workflow.on_update('register_callback', validator: lambda { |options|
        self.class.futures[options[:state]].public_send(options[:callback]) do
          self.class.callback_runs += 1
          workflow.complete('must not close the workflow')
        end
      }) { 'must not accept the update' }
      workflow.on_update('finish') { @finished = true }
      workflow.wait_until { @finished }
      'workflow-result'
    end
  end

  before do
    UpdateTestWorkflow.validations = UpdateTestWorkflow.executions = 0
    UpdateTestWorkflow.reject = UpdateTestWorkflow.blocking_validator = false
  end

  def message(name = 'append', id: 'u1', sequence: 2, input: 'one', headers: {})
    request = Temporalio::Api::Update::V1::Request.new(
      meta: Temporalio::Api::Update::V1::Meta.new(update_id: id),
      input: Temporalio::Api::Update::V1::Input.new(name: name, args: converter.to_payloads([input]),
        header: Temporalio::Api::Common::V1::Header.new(fields: converter.to_payload_map(headers)))
    )
    Temporalio::Api::Protocol::V1::Message.new(id: "#{id}/request", protocol_instance_id: id,
      event_id: sequence, body: Google::Protobuf::Any.pack(request))
  end

  def run(events, messages = [], workflow = UpdateTestWorkflow)
    history = Temporal::Workflow::History.new(events, messages: messages)
    Temporal::Workflow::Executor.new(workflow, history, metadata, config, false, Temporal::Middleware::Chain.new).run
  end

  def command_events(result, start_id: 5)
    result.commands.map.with_index do |(_, command), index|
      raise 'only protocol commands supported by helper' unless command.is_a?(Temporal::Workflow::Command::ProtocolMessage)
      proto = command.message
      if proto.body.type_url.end_with?('Acceptance')
        acceptance = Temporalio::Api::Update::V1::Acceptance.decode(proto.body.value)
        attrs = Temporalio::Api::History::V1::WorkflowExecutionUpdateAcceptedEventAttributes.new(
          protocol_instance_id: proto.protocol_instance_id,
          accepted_request_message_id: acceptance.accepted_request_message_id,
          accepted_request_sequencing_event_id: acceptance.accepted_request_sequencing_event_id,
          accepted_request: acceptance.accepted_request
        )
        type = 'accepted'
      else
        response = Temporalio::Api::Update::V1::Response.decode(proto.body.value)
        attrs = Temporalio::Api::History::V1::WorkflowExecutionUpdateCompletedEventAttributes.new(
          meta: response.meta, outcome: response.outcome)
        type = 'completed'
      end
      Temporalio::Api::History::V1::HistoryEvent.new(event_id: start_id + index, event_time: Time.now,
        event_type: "EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_#{type.upcase}".to_sym,
        "workflow_execution_update_#{type}_event_attributes" => attrs)
    end
  end

  def outcome(result)
    Temporalio::Api::Update::V1::Response.decode(result.messages.last.body.value).outcome
  end

  it 'records ordered acceptance and completion, then replays without validation or duplicate commands' do
    result = run(events, [message])
    expect(result.commands.map(&:first)).to eq([5, 6])
    expect(result.messages.map(&:id)).to eq(['u1/accept', 'u1/complete'])
    expect(converter.from_result_payloads(outcome(result).success)).to eq(['one'])
    expect(UpdateTestWorkflow.executions).to eq(1)
    replay = events + [Fabricate(:api_workflow_task_completed_event, event_id: 4)] + command_events(result)
    UpdateTestWorkflow.reject = true
    result = run(replay)
    expect(result.commands).to be_empty
    expect(result.messages).to be_empty
    expect(UpdateTestWorkflow.validations).to eq(1)
    expect(UpdateTestWorkflow.executions).to eq(2)
  end

  it 'delivers a reset-admitted request from history even without a live protocol message' do
    request = Temporalio::Api::Update::V1::Request.decode(message.body.value)
    admitted = Temporalio::Api::History::V1::HistoryEvent.new(event_id: 2, event_time: Time.now,
      event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED,
      workflow_execution_update_admitted_event_attributes: Temporalio::Api::History::V1::WorkflowExecutionUpdateAdmittedEventAttributes.new(request: request, origin: 1))
    reset_events = [events.first, admitted, Fabricate(:api_workflow_task_scheduled_event, event_id: 3),
      Fabricate(:api_workflow_task_started_event, event_id: 4)]
    result = run(reset_events)
    expect(result.messages.map(&:id)).to eq(['u1/accept', 'u1/complete'])
    expect(UpdateTestWorkflow.validations).to eq(1)
    recorded = command_events(result, start_id: 6)
    # Server omits the duplicate request when it already exists in admission history.
    recorded.first.workflow_execution_update_accepted_event_attributes.accepted_request = nil
    replay = reset_events + [Fabricate(:api_workflow_task_completed_event, event_id: 5)] + recorded
    expect(run(replay).commands).to be_empty
    expect(UpdateTestWorkflow.validations).to eq(1)
  end

  it 'rejects a header decoding error before acceptance, including redelivery, without leaking an active handler' do
    invalid = message(headers: { 'invalid' => 'header' })
    allow(converter).to receive(:from_payload_map).and_wrap_original do |original, fields|
      raise ArgumentError, 'cannot decode header' if fields['invalid']
      original.call(fields)
    end
    2.times do
      result = run(events, [invalid, message('finish', id: 'u2')])
      expect(result.messages.map(&:id)).to match_array(['u1/reject', 'u2/accept', 'u2/complete'])
      rejection = Temporalio::Api::Update::V1::Rejection.decode(result.messages.find { |item| item.id == 'u1/reject' }.body.value)
      expect(rejection.failure.message).to eq('cannot decode header')
      # Main completion is only emitted when active_count is zero.
      expect(result.commands.last.last).to be_a(Temporal::Workflow::Command::CompleteWorkflow)
      expect(UpdateTestWorkflow.validations).to eq(0)
      expect(UpdateTestWorkflow.executions).to eq(0)
    end
  end

  it 'fails accepted replay as nondeterministic if its recorded headers can no longer be decoded' do
    first = run(events, [message(headers: { 'changed' => 'header' })])
    replay = events + [Fabricate(:api_workflow_task_completed_event, event_id: 4)] + command_events(first)
    allow(converter).to receive(:from_payload_map).and_wrap_original do |original, fields|
      raise ArgumentError, 'cannot decode header' if fields['changed']
      original.call(fields)
    end
    expect { run(replay) }.to raise_error(Temporal::NonDeterministicWorkflowError, /Cannot replay accepted Update: cannot decode header/)
  end

  # Exercise deferred registration and both immediate-callback paths without
  # repeating the Cartesian product: each method calls the same pre-branch guard.
  [[:done, :pending], [:done, :ready], [:failed, :failed]].each do |callback, state|
    it "rejects validator #{callback} registration on a #{state} Future without executing or saving the callback, and replays" do
      invalid = message('register_callback', input: { callback: callback, state: state })
      result = run(events, [invalid, message('finish', id: 'u2')], UpdateValidatorCallbackWorkflow)
      expect(result.messages.map(&:id)).to match_array(['u1/reject', 'u2/accept', 'u2/complete'])
      rejection = Temporalio::Api::Update::V1::Rejection.decode(result.messages.find { |item| item.id == 'u1/reject' }.body.value)
      expect(rejection.failure.message).to eq('Update validators must not register Future callbacks')
      future = UpdateValidatorCallbackWorkflow.futures[state]
      expect(future.success_callbacks + future.failure_callbacks).to be_empty
      expect(UpdateValidatorCallbackWorkflow.callback_runs).to eq(0)
      expect(result.commands.last.last).to be_a(Temporal::Workflow::Command::CompleteWorkflow)
      updates = Temporal::Workflow::Executor::RunResult.new(commands: result.commands[0..1])
      replay = events + [Fabricate(:api_workflow_task_completed_event, event_id: 4)] + command_events(updates) +
        [Fabricate(:api_workflow_execution_completed_event, event_id: 7)]
      expect(run(replay, [], UpdateValidatorCallbackWorkflow).commands).to be_empty
    end
  end

  it 'rejects unknown handlers' do
    result = run(events, [message('unknown')])
    rejection = Temporalio::Api::Update::V1::Rejection.decode(result.messages.first.body.value)
    expect(rejection.failure.message).to include('Unknown Update handler')
  end

  it 'prevents validators from scheduling timers' do
    UpdateTestWorkflow.blocking_validator = true
    result = run(events, [message])
    expect(result.commands).to be_empty
    expect(result.messages.first.id).to eq('u1/reject')
  end

  it 'rejects a ScriptError from a validator without failing the workflow task' do
    result = run(events, [message('script_validator')], UpdateExceptionTestWorkflow)
    expect(result.commands).to be_empty
    expect(result.messages.map(&:id)).to eq(['u1/reject'])
    rejection = Temporalio::Api::Update::V1::Rejection.decode(result.messages.first.body.value)
    expect(rejection.failure.application_failure_info.type).to eq('NotImplementedError')
  end

  it 'completes an accepted Update with failure when its handler raises ScriptError, and replays it' do
    result = run(events, [message('script_handler')], UpdateExceptionTestWorkflow)
    expect(result.messages.map(&:id)).to eq(['u1/accept', 'u1/complete'])
    expect(outcome(result).failure.application_failure_info.type).to eq('NotImplementedError')
    replay = events + [Fabricate(:api_workflow_task_completed_event, event_id: 4)] + command_events(result)
    expect(run(replay, [], UpdateExceptionTestWorkflow).commands).to be_empty
  end

  it 'deduplicates repeated messages and preserves order between distinct updates' do
    result = run(events, [message, message, message(id: 'u2', input: 'two')])
    expect(result.commands.map(&:first)).to eq([5, 6, 7, 8])
    expect(converter.from_result_payloads(outcome(result).success)).to eq(['one', 'two'])
    expect(UpdateTestWorkflow.executions).to eq(2)
  end

  it 'automatically defers main-workflow completion across tasks until a blocked Update finishes, then replays both completions' do
    first = run(events, [message('delayed')], UpdateAutoCompletionWorkflow)
    expect(first.commands.map { |_, command| command.class }).to eq([
      Temporal::Workflow::Command::ProtocolMessage, Temporal::Workflow::Command::StartTimer])
    accepted = command_events(Temporal::Workflow::Executor::RunResult.new(commands: [first.commands.first])).first
    timer_start = Fabricate(:api_timer_started_event, event_id: 6)
    timer_fired = Fabricate(:api_timer_fired_event, event_id: 7,
      timer_fired_event_attributes: Temporalio::Api::History::V1::TimerFiredEventAttributes.new(timer_id: '6', started_event_id: 6))
    next_task = events + [Fabricate(:api_workflow_task_completed_event, event_id: 4), accepted, timer_start,
      timer_fired, Fabricate(:api_workflow_task_scheduled_event, event_id: 8), Fabricate(:api_workflow_task_started_event, event_id: 9)]
    second = run(next_task, [], UpdateAutoCompletionWorkflow)
    expect(second.commands.map(&:first)).to eq([11, 12])
    expect(second.messages.map(&:id)).to eq(['u1/complete'])
    expect(converter.from_result_payloads(outcome(second).success)).to eq('update-result')
    expect(second.commands.last.last.result).to eq('workflow-result')
    update_completed = command_events(Temporal::Workflow::Executor::RunResult.new(commands: [second.commands.first]), start_id: 11).first
    finished = next_task + [Fabricate(:api_workflow_task_completed_event, event_id: 10), update_completed,
      Fabricate(:api_workflow_execution_completed_event, event_id: 12)]
    expect(run(finished, [], UpdateAutoCompletionWorkflow).commands).to be_empty
  end

  it 'does not resume an Update twice when nested completion dispatch wakes a wait condition' do
    result = run(events, [message('blocked'), message('release', id: 'u2')])
    expect(result.messages.map(&:id)).to eq(['u1/accept', 'u2/accept', 'u2/complete'])
    expect(result.commands.last.last).to be_a(Temporal::Workflow::Command::StartTimer)
  end

end

describe 'Workflow Update client' do
  let(:config) { Temporal::Configuration.new }
  let(:converter) { config.converter }
  let(:connection) { Temporal::Connection.generate(config.for_connection) }
  let(:stub) { double('workflow service') }
  let(:client) { Temporal::Client.new(config) }
  let(:service) { Temporalio::Api::WorkflowService::V1 }
  let(:ref) do
    Temporalio::Api::Update::V1::UpdateRef.new(update_id: 'update-id',
      workflow_execution: Temporalio::Api::Common::V1::WorkflowExecution.new(workflow_id: 'workflow-id', run_id: 'run-id'))
  end
  let(:accepted) { service::UpdateWorkflowExecutionResponse.new(update_ref: ref, stage: 2) }
  let(:completed) do
    service::UpdateWorkflowExecutionResponse.new(update_ref: ref, stage: 3,
      outcome: Temporalio::Api::Update::V1::Outcome.new(success: converter.to_result_payloads('result')))
  end

  before do
    allow(client).to receive(:connection).and_return(connection)
    allow(connection).to receive(:client).and_return(stub)
  end

  def multi_response(update, started: true)
    service::ExecuteMultiOperationResponse.new(responses: [
      service::ExecuteMultiOperationResponse::Response.new(start_workflow: service::StartWorkflowExecutionResponse.new(run_id: 'run-id', started: started)),
      service::ExecuteMultiOperationResponse::Response.new(update_workflow: update)
    ])
  end

  it 'uses a single ordered atomic multi-operation and keeps arguments and IDs distinct' do
    options = { workflow_id: 'workflow-id', workflow_id_reuse_policy: :reject, task_queue: 'billing' }.freeze
    expect(stub).to receive(:execute_multi_operation) do |request, rpc_options|
      expect(rpc_options[:deadline]).to be > Time.now
      expect(request.operations.size).to eq(2)
      start = request.operations[0].start_workflow
      update = request.operations[1].update_workflow
      expect(start.workflow_id).to eq('workflow-id')
      expect(start.workflow_id_conflict_policy).to eq(:WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING)
      expect(start.workflow_id_reuse_policy).to eq(:WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE)
      expect(start.request_id).to eq('start-request-id')
      expect(converter.from_payloads(start.input)).to eq(['account'])
      expect(update.workflow_execution.workflow_id).to eq(start.workflow_id)
      expect(update.workflow_execution.run_id).to eq('')
      expect(update.request.meta.update_id).to eq('update-id')
      expect(converter.from_payloads(update.request.input.args)).to eq(['invoice'])
      expect(update.wait_policy.lifecycle_stage).to eq(:UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED)
      multi_response(completed)
    end
    result = client.execute_update_with_start_workflow('Billing', 'cancel', 'invoice', start_args: ['account'],
      options: options, update_id: 'update-id', request_id: 'start-request-id', workflow_id_conflict_policy: :use_existing)
    expect(result).to eq('result')
  end

  it 'returns an accepted handle pinned to the selected run and waits for a completion outcome' do
    allow(stub).to receive(:execute_multi_operation).and_return(multi_response(accepted, started: false))
    handle = client.start_update_with_start_workflow('Billing', 'cancel', options: { workflow_id: 'workflow-id' },
      workflow_id_conflict_policy: :use_existing)
    expect([handle.workflow_id, handle.run_id, handle.update_id, handle.workflow_started]).to eq(['workflow-id', 'run-id', 'update-id', false])
    calls = 0
    expect(stub).to receive(:poll_workflow_execution_update).twice do |request, rpc_options|
      expect(request.update_ref).to eq(ref)
      calls += 1
      service::PollWorkflowExecutionUpdateResponse.new(stage: calls == 1 ? 2 : 3, outcome: calls == 1 ? nil : completed.outcome)
    end
    expect(handle.result).to eq('result')
    expect(handle.result).to eq('result')
  end

  it 'retries a transient RPC failure and an early server response with the exact same IDs' do
    requests = []
    allow(connection).to receive(:sleep)
    expect(stub).to receive(:execute_multi_operation).exactly(3).times do |request, **|
      requests << request
      raise GRPC::Unavailable, 'connection lost' if requests.size == 1
      multi_response(requests.size == 2 ? service::UpdateWorkflowExecutionResponse.new(stage: 0) : accepted, started: requests.size == 2)
    end
    handle = client.start_update_with_start_workflow('Billing', 'cancel', options: { workflow_id: 'workflow-id' },
      update_id: 'update-id', request_id: 'request-id', workflow_id_conflict_policy: :use_existing)
    expect(handle.workflow_started).to eq(true)
    expect(requests.map(&:object_id).uniq.size).to eq(1)
    expect(requests.first.operations.first.start_workflow.request_id).to eq('request-id')
  end

  it 'supports plain Update requests with a first-run guard and headers' do
    expect(stub).to receive(:update_workflow_execution) do |request, **|
      expect(request.first_execution_run_id).to eq('first-run')
      expect(request.workflow_execution.run_id).to eq('run-id')
      expect(converter.from_payload_map(request.request.input.header.fields)).to eq('trace' => 'context')
      completed
    end
    expect(client.execute_workflow_update(nil, 'cancel', 'workflow-id', 'run-id', 'arg',
      update_id: 'update-id', first_execution_run_id: 'first-run', headers: { 'trace' => 'context' })).to eq('result')
  end

  it 'returns validator and handler failures through the handle with their cause' do
    failure = Temporal::Connection::Serializer::Failure.new(ArgumentError.new('bad invoice'), converter).to_proto
    rejected = service::UpdateWorkflowExecutionResponse.new(update_ref: ref, stage: 3,
      outcome: Temporalio::Api::Update::V1::Outcome.new(failure: failure))
    allow(stub).to receive(:update_workflow_execution).and_return(rejected)
    expect { client.execute_workflow_update(nil, 'cancel', 'workflow-id') }.to raise_error(Temporal::WorkflowUpdateFailed) do |e|
      expect(e.cause).to be_a(ArgumentError)
      expect(e.update_id).to eq('update-id')
    end
  end

  it 'rejects invalid stages, missing workflow IDs and unsupported start options before any RPC' do
    expect { client.start_workflow_update(nil, 'u', 'w', wait_for_stage: :admitted) }.to raise_error(ArgumentError)
    expect { client.start_update_with_start_workflow('W', 'u', workflow_id_conflict_policy: :use_existing) }.to raise_error(ArgumentError)
    expect { client.start_update_with_start_workflow('W', 'u', workflow_id_conflict_policy: :use_existing,
      options: { workflow_id: 'w', start_delay: 1 }) }.to raise_error(ArgumentError)
  end

  it 'does not retry permanent multi-operation errors' do
    expect(stub).to receive(:execute_multi_operation).once.and_raise(GRPC::InvalidArgument, 'invalid')
    expect { client.start_update_with_start_workflow('W', 'u', options: { workflow_id: 'w' },
      workflow_id_conflict_policy: :fail) }.to raise_error(GRPC::InvalidArgument)
  end

  it 'bounds waiting without claiming the update was canceled' do
    allow(stub).to receive(:update_workflow_execution).and_raise(GRPC::DeadlineExceeded, 'deadline')
    expect { client.start_workflow_update(nil, 'u', 'w', timeout: 0.005) }.to raise_error(Temporal::TimeoutError, /may still be running/)
  end
end

require 'workflows/hello_world_workflow'
require 'workflows/long_workflow'
require 'workflows/wait_for_external_signal_workflow'

describe 'Temporal.start_workflow', :integration do
  let(:workflow_id) { SecureRandom.uuid }

  it 'starts a workflow using a class reference' do
    run_id = Temporal.start_workflow(HelloWorldWorkflow, 'Test', options: {
      workflow_id: workflow_id
    })

    result = Temporal.await_workflow_result(
      HelloWorldWorkflow,
      workflow_id: workflow_id,
      run_id: run_id
    )

    expect(result).to eq('Hello World, Test')
  end

  it 'starts a workflow using a string reference' do
    run_id = Temporal.start_workflow('HelloWorldWorkflow', 'Test', options: {
      workflow_id: workflow_id,
      namespace: integration_spec_namespace,
      task_queue: integration_spec_task_queue
    })

    result = Temporal.await_workflow_result(
      'HelloWorldWorkflow',
      workflow_id: workflow_id,
      run_id: run_id,
      namespace: integration_spec_namespace
    )

    expect(result).to eq('Hello World, Test')
  end

  it 'returns the running execution for :use_existing without delivering the new input' do
    original_run_id = Temporal.start_workflow(WaitForExternalSignalWorkflow, 'original',
      options: { workflow_id: workflow_id })

    expect do
      Temporal.start_workflow(WaitForExternalSignalWorkflow, 'replacement', options: { workflow_id: workflow_id })
    end.to raise_error(Temporal::WorkflowExecutionAlreadyStartedFailure)

    returned_run_id = Temporal.start_workflow(WaitForExternalSignalWorkflow, 'replacement',
      options: { workflow_id: workflow_id, workflow_id_conflict_policy: :use_existing })

    expect(returned_run_id).to eq(original_run_id)
    started = fetch_history(workflow_id, original_run_id).history.events.find do |event|
      event.event_type == :EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
    end
    expect(started.workflow_execution_started_event_attributes.input.payloads.map(&:data)).to eq(['"original"'])

    Temporal.signal_workflow(WaitForExternalSignalWorkflow, 'original', workflow_id, original_run_id, 'received')
    result = Temporal.await_workflow_result(WaitForExternalSignalWorkflow,
      workflow_id: workflow_id, run_id: original_run_id)
    expect(result).to eq(received: { 'original' => 'received' }, counts: { 'original' => 1 })
  end

  it 'uses the reuse policy after the previous run closes' do
    original_run_id = Temporal.start_workflow(LongWorkflow,
      options: { workflow_id: workflow_id })
    Temporal.terminate_workflow(workflow_id, run_id: original_run_id)

    expect do
      Temporal.start_workflow(LongWorkflow, options: {
        workflow_id: workflow_id, workflow_id_reuse_policy: :reject,
        workflow_id_conflict_policy: :use_existing
      })
    end.to raise_error(Temporal::WorkflowExecutionAlreadyStartedFailure)

    new_run_id = Temporal.start_workflow(LongWorkflow, options: {
      workflow_id: workflow_id, workflow_id_reuse_policy: :allow,
      workflow_id_conflict_policy: :use_existing
    })
    expect(new_run_id).not_to eq(original_run_id)
  ensure
    Temporal.terminate_workflow(workflow_id, run_id: new_run_id) if new_run_id
  end

  it 'rejects duplicate workflow ids based on workflow_id_reuse_policy' do
    # Run it once...
    run_id = Temporal.start_workflow(HelloWorldWorkflow, 'Test', options: {
      workflow_id: workflow_id,
    })

    result = Temporal.await_workflow_result(
      HelloWorldWorkflow,
      workflow_id: workflow_id,
      run_id: run_id
    )

    expect(result).to eq('Hello World, Test')

    expect do
      Temporal.start_workflow(HelloWorldWorkflow, 'Test', options: {
        workflow_id: workflow_id,
        workflow_id_reuse_policy: :allow_failed,
        workflow_id_conflict_policy: :use_existing
      })
    end.to raise_error(Temporal::WorkflowExecutionAlreadyStartedFailure)

    # And again, allowing duplicates...
    run_id = Temporal.start_workflow(HelloWorldWorkflow, 'Test', options: {
      workflow_id: workflow_id,
      workflow_id_reuse_policy: :allow
    })

    Temporal.await_workflow_result(
      HelloWorldWorkflow,
      workflow_id: workflow_id,
      run_id: run_id
    )

    # And again, rejecting duplicates...
    expect do
      Temporal.start_workflow(HelloWorldWorkflow, 'Test', options: {
        workflow_id: workflow_id,
        workflow_id_reuse_policy: :reject
      })
    end.to raise_error(Temporal::WorkflowExecutionAlreadyStartedFailure)
  end

  it 'terminates duplicate workflow ids based on workflow_id_reuse_policy' do
    run_id_1 = Temporal.start_workflow(LongWorkflow, options: {
      workflow_id: workflow_id,
      workflow_id_reuse_policy: :terminate_if_running
    })

    run_id_2 = Temporal.start_workflow(LongWorkflow, options: {
      workflow_id: workflow_id,
      workflow_id_reuse_policy: :terminate_if_running
    })

    execution_1 = Temporal.fetch_workflow_execution_info(
      integration_spec_namespace,
      workflow_id,
      run_id_1)
    execution_2 = Temporal.fetch_workflow_execution_info(
      integration_spec_namespace,
      workflow_id,
      run_id_2)

    expect(execution_1.status).to eq(Temporal::Workflow::Status::TERMINATED)
    expect(execution_2.status).to eq(Temporal::Workflow::Status::RUNNING)
  end
end

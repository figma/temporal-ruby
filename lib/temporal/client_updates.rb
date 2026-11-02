require 'temporal/update_handle'

module Temporal
  module ClientUpdates
    # Start an Update on a specific execution. update_id deduplicates requests
    # within a run; use a stable ID when retrying after an ambiguous RPC failure.
    def start_workflow_update(workflow, update, workflow_id, run_id = nil, *input,
                              update_id: SecureRandom.uuid, wait_for_stage: :accepted,
                              namespace: nil, headers: {}, first_execution_run_id: nil, timeout: 60)
      execution = ExecutionOptions.new(workflow, {}, config.default_execution_options)
      namespace ||= execution.namespace
      response = connection.update_workflow_execution(
        namespace: namespace, workflow_id: workflow_id, run_id: run_id,
        update: update, input: input, update_id: update_id, wait_for_stage: wait_for_stage,
        headers: config.header_propagator_chain.inject(headers),
        first_execution_run_id: first_execution_run_id, timeout: timeout
      )
      update_handle_from(response, namespace)
    end

    def execute_workflow_update(workflow, update, workflow_id, run_id = nil, *input, **options)
      start_workflow_update(workflow, update, workflow_id, run_id, *input,
                            **options.merge(wait_for_stage: :completed)).result
    end

    # Atomically start (or select) a Workflow and submit its Update. A conflict
    # policy is mandatory: :use_existing or :fail. options[:workflow_id] is required.
    # start_args belong to Workflow#execute; input belongs to the Update handler.
    def start_update_with_start_workflow(workflow, update, *input, start_args: [], options: {},
                                         workflow_id_conflict_policy:, update_id: SecureRandom.uuid,
                                         request_id: SecureRandom.uuid, wait_for_stage: :accepted,
                                         headers: {}, timeout: 60)
      workflow_id = options[:workflow_id]
      raise ArgumentError, 'options[:workflow_id] is required' unless workflow_id.is_a?(String) && !workflow_id.empty?
      unless [:use_existing, :fail].include?(workflow_id_conflict_policy)
        raise ArgumentError, 'workflow_id_conflict_policy must be :use_existing or :fail'
      end
      if options.values_at(:signal_name, :signal_input, :cron_schedule, :start_delay).any?
        raise ArgumentError, 'Update-With-Start does not support signals, cron schedules or start delays'
      end
      execution = ExecutionOptions.new(workflow, options, config.default_execution_options)
      response = connection.update_with_start_workflow_execution(
        namespace: execution.namespace, workflow_id: workflow_id,
        workflow_name: execution.name, task_queue: execution.task_queue, input: start_args,
        execution_timeout: execution.timeouts[:execution], run_timeout: compute_run_timeout(execution),
        task_timeout: execution.timeouts[:task], workflow_id_reuse_policy: options[:workflow_id_reuse_policy],
        workflow_id_conflict_policy: workflow_id_conflict_policy, request_id: request_id,
        headers: config.header_propagator_chain.inject(execution.headers), memo: execution.memo,
        search_attributes: Workflow::Context::Helpers.process_search_attributes(execution.search_attributes),
        retry_policy: execution.retry_policy,
        update: update, update_input: input, update_id: update_id,
        update_headers: config.header_propagator_chain.inject(headers), wait_for_stage: wait_for_stage, timeout: timeout
      )
      update_handle_from(response.responses[1].update_workflow, execution.namespace,
                         workflow_started: response.responses[0].start_workflow.started)
    end

    def execute_update_with_start_workflow(workflow, update, *input, **options)
      start_update_with_start_workflow(workflow, update, *input,
                                       **options.merge(wait_for_stage: :completed)).result
    end

    # Reattach after a client restart. Always save and use the returned run_id.
    def get_workflow_update_handle(workflow_id, run_id, update_id, namespace: config.namespace)
      WorkflowUpdateHandle.new(connection: connection, converter: converter, namespace: namespace,
                               workflow_id: workflow_id, run_id: run_id, update_id: update_id)
    end

    private

    def update_handle_from(response, namespace, workflow_started: nil)
      ref = response.update_ref
      WorkflowUpdateHandle.new(connection: connection, converter: converter, namespace: namespace,
                               workflow_id: ref.workflow_execution.workflow_id, run_id: ref.workflow_execution.run_id,
                               update_id: ref.update_id, outcome: response.outcome, workflow_started: workflow_started)
    end
  end
end

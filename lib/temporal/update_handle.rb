require 'temporal/errors'
require 'temporal/workflow/errors'

module Temporal
  # A validator rejection or handler failure affects the Update, not the Workflow.
  class WorkflowUpdateFailed < StandardError
    attr_reader :failure, :update_id

    def initialize(failure, update_id)
      @failure = failure
      @update_id = update_id
      super(failure.message)
    end
  end

  class WorkflowUpdateHandle
    attr_reader :namespace, :workflow_id, :run_id, :update_id, :workflow_started

    def initialize(connection:, converter:, namespace:, workflow_id:, run_id:, update_id:, outcome: nil, workflow_started: nil)
      @connection, @converter = connection, converter
      @namespace, @workflow_id, @run_id, @update_id = namespace, workflow_id, run_id, update_id
      @outcome = outcome
      @workflow_started = workflow_started
    end

    # Waiting does not cancel an Update when the timeout expires. The handle can
    # be saved (namespace/workflow_id/run_id/update_id) and retrieved later.
    def result(timeout: 60)
      @outcome ||= @connection.poll_workflow_update(
        namespace: namespace, workflow_id: workflow_id, run_id: run_id,
        update_id: update_id, timeout: timeout
      ).outcome
      if @outcome.failure
        cause = Workflow::Errors.generate_error(@outcome.failure, @converter)
        raise WorkflowUpdateFailed.new(@outcome.failure, update_id), cause: cause
      end
      @converter.from_result_payloads(@outcome.success)
    end
  end
end

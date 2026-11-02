module Temporal
  module Connection
    module WorkflowUpdates
      UPDATE_STAGES = { accepted: 2, completed: 3 }.freeze

      def update_workflow_execution(namespace:, workflow_id:, run_id:, update:, input:, update_id:,
                                    wait_for_stage:, headers:, first_execution_run_id:, timeout:)
        request = build_update_request(namespace: namespace, workflow_id: workflow_id, run_id: run_id,
                                       update: update, input: input, update_id: update_id,
                                       wait_for_stage: wait_for_stage, headers: headers,
                                       first_execution_run_id: first_execution_run_id)
        wait_for_update(timeout) do |deadline|
          response = client.update_workflow_execution(request, deadline: deadline)
          response if update_stage_reached?(response, wait_for_stage)
        end
      end

      def update_with_start_workflow_execution(update:, update_input:, update_id:, update_headers:,
                                               wait_for_stage:, timeout:, **start_options)
        start = build_start_workflow_execution_request(**start_options)
        update = build_update_request(namespace: start.namespace, workflow_id: start.workflow_id,
                                      run_id: nil, update: update, input: update_input, update_id: update_id,
                                      wait_for_stage: wait_for_stage, headers: update_headers,
                                      first_execution_run_id: nil)
        request = Temporalio::Api::WorkflowService::V1::ExecuteMultiOperationRequest.new(
          namespace: start.namespace, operations: [
            Temporalio::Api::WorkflowService::V1::ExecuteMultiOperationRequest::Operation.new(start_workflow: start),
            Temporalio::Api::WorkflowService::V1::ExecuteMultiOperationRequest::Operation.new(update_workflow: update)
          ]
        )
        start_response = nil
        wait_for_update(timeout) do |deadline|
          response = client.execute_multi_operation(request, deadline: deadline)
          # Subsequent long polls reuse the same start request and may report
          # started=false. Retain the initial successful start response.
          start_response ||= response.responses.fetch(0).start_workflow
          response.responses[0].start_workflow = start_response
          response if update_stage_reached?(response.responses.fetch(1).update_workflow, wait_for_stage)
        end
      end

      def poll_workflow_update(namespace:, workflow_id:, run_id:, update_id:, timeout:)
        request = Temporalio::Api::WorkflowService::V1::PollWorkflowExecutionUpdateRequest.new(
          namespace: namespace, identity: identity,
          update_ref: Temporalio::Api::Update::V1::UpdateRef.new(
            workflow_execution: Temporalio::Api::Common::V1::WorkflowExecution.new(workflow_id: workflow_id, run_id: run_id),
            update_id: update_id
          ), wait_policy: Temporalio::Api::Update::V1::WaitPolicy.new(lifecycle_stage: UPDATE_STAGES[:completed])
        )
        wait_for_update(timeout) do |deadline|
          response = client.poll_workflow_execution_update(request, deadline: deadline)
          response if response.outcome
        end
      end

      private

      def build_update_request(namespace:, workflow_id:, run_id:, update:, input:, update_id:,
                               wait_for_stage:, headers:, first_execution_run_id:)
        raise ::ArgumentError, 'update_id must be a nonempty String' unless update_id.is_a?(String) && !update_id.empty?
        raise ::ArgumentError, 'update must be a nonempty name' if update.to_s.empty?
        stage = UPDATE_STAGES.fetch(wait_for_stage) { raise ::ArgumentError, 'wait_for_stage must be :accepted or :completed' }
        Temporalio::Api::WorkflowService::V1::UpdateWorkflowExecutionRequest.new(
          namespace: namespace,
          workflow_execution: Temporalio::Api::Common::V1::WorkflowExecution.new(workflow_id: workflow_id, run_id: run_id),
          first_execution_run_id: first_execution_run_id,
          wait_policy: Temporalio::Api::Update::V1::WaitPolicy.new(lifecycle_stage: stage),
          request: Temporalio::Api::Update::V1::Request.new(
            meta: Temporalio::Api::Update::V1::Meta.new(update_id: update_id, identity: identity),
            input: Temporalio::Api::Update::V1::Input.new(name: update.to_s, args: converter.to_payloads(input),
                                                       header: Temporalio::Api::Common::V1::Header.new(fields: converter.to_payload_map(headers)))
          )
        )
      end

      def update_stage_reached?(response, stage)
        response.outcome || Temporalio::Api::Enums::V1::UpdateWorkflowExecutionLifecycleStage.resolve(response.stage) >= UPDATE_STAGES.fetch(stage)
      end

      # A server long poll may return before the requested stage. Retry the exact
      # same immutable request, including both IDs, within a single timeout budget.
      def wait_for_update(timeout)
        raise ::ArgumentError, 'timeout must be positive' unless timeout.is_a?(Numeric) && timeout.positive? && timeout.finite?
        expires = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
        loop do
          remaining = expires - Process.clock_gettime(Process::CLOCK_MONOTONIC)
          raise Temporal::TimeoutError, 'Timed out waiting for Update; the Update may still be running' if remaining <= 0
          begin
            response = yield(Time.now + remaining)
            return response if response
          rescue ::GRPC::Unavailable, ::GRPC::ResourceExhausted, ::GRPC::DeadlineExceeded
            remaining = expires - Process.clock_gettime(Process::CLOCK_MONOTONIC)
            sleep([0.1, remaining].min) if remaining.positive?
          end
        end
      end
    end
  end
end

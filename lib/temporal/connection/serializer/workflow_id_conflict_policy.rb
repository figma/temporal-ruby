require 'temporal/connection'

module Temporal
  module Connection
    module Serializer
      class WorkflowIdConflictPolicy < Base
        WORKFLOW_ID_CONFLICT_POLICY = {
          fail: Temporalio::Api::Enums::V1::WorkflowIdConflictPolicy::WORKFLOW_ID_CONFLICT_POLICY_FAIL,
          use_existing: Temporalio::Api::Enums::V1::WorkflowIdConflictPolicy::WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
        }.freeze

        def to_proto
          return if object.nil?

          policy = WORKFLOW_ID_CONFLICT_POLICY[object]
          raise ArgumentError, "Unknown workflow_id_conflict_policy specified: #{object}" unless policy

          policy
        end
      end
    end
  end
end

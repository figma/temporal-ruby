module Temporal
  module Testing
    # In local mode, Temporal.start_workflow normally runs the workflow inline and
    # synchronously, which collapses the start/run distinction that exists against a
    # real Temporal server (start_workflow enqueues; the workflow runs later, out of
    # process). That inline execution is wrong for a caller that holds a resource --
    # e.g. a lock -- across the start_workflow call and expects the workflow to run
    # only after it has returned and released that resource.
    #
    # Inside a Temporal::Testing.with_deferred_starts block, start_workflow instead
    # defers execution: the workflow is queued and run when the block exits, after the
    # caller's stack has unwound. This lets a test model the real async ordering.
    #
    # Awaited workflows cannot be deferred: await_workflow_result needs the result
    # synchronously, and local mode does not capture a workflow's return value. Only
    # fire-and-forget start_workflow calls should run inside the block.
    module DeferredStarts
      def self.execute_all
        Private::Store.execute_all
      end

      def self.clear_all
        Private::Store.clear_all
      end

      module Private
        module Store
          class << self
            def add(executor_lambda:)
              executions << executor_lambda
            end

            def execute_all
              # Drain FIFO. The defer flag is cleared before we drain, so any workflow
              # started during a drain runs inline and the queue does not grow here.
              #
              # Each runs independently, mirroring separate executions on a real worker:
              # WorkflowExecution#run records a failed run as FAILED rather than raising,
              # so one workflow failing does not abort the drain of the others.
              executions.shift.call until executions.empty?
            end

            def clear_all
              @executions = []
            end

            private

            def executions
              @executions ||= []
            end
          end
        end
      end
    end
  end
end

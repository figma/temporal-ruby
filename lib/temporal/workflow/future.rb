require 'fiber'
require 'temporal/workflow/update_execution'

module Temporal
  class Workflow
    class Future
      attr_reader :target, :success_callbacks, :failure_callbacks

      def initialize(target, context, cancelation_id: nil)
        @target = target
        @context = context
        @cancelation_id = cancelation_id
        @success_callbacks = []
        @failure_callbacks = []
        @ready = false
        @failed = false
        @result = nil
        @exception = nil
      end

      def finished?
        ready? || failed?
      end

      def ready?
        @ready
      end

      def failed?
        @failed
      end

      def wait
        return if finished?
        context.wait_for_any(self)
      end

      def get
        wait
        exception || result
      end

      def set(result)
        raise 'can not fulfil a failed future' if failed?

        @result = result
        @ready = true
        failure_callbacks.grep(UpdateExecution::Callback).each(&:discard)
      end

      def fail(exception)
        raise 'can not fail a fulfilled future' if ready?

        @exception = exception
        @failed = true
        success_callbacks.grep(UpdateExecution::Callback).each(&:discard)
      end

      # When the activity completes successfully, the block will be called with any result
      def done(&block)
        block = own_update_callback(block)
        if ready?
          block.call(result)
        else
          # If the future is still outstanding, schedule a callback for invocation by the
          # workflow context when the workflow or activity is finished
          success_callbacks << block
          block.discard if failed? && block.is_a?(UpdateExecution::Callback)
        end
      end

      # When the activity fails, the block will be called with the exception
      def failed(&block)
        block = own_update_callback(block)
        if failed?
          block.call(exception)
        else
          # If the future is still outstanding, schedule a callback for invocation by the
          # workflow context when the workflow or activity is finished
          failure_callbacks << block
          block.discard if ready? && block.is_a?(UpdateExecution::Callback)
        end
      end

      def cancel
        return false if finished?

        context.cancel(target, cancelation_id)
      end

      private

      attr_reader :context, :cancelation_id, :result, :exception

      def own_update_callback(block)
        # Registration belongs to the active caller, not to the context that
        # created the Future. Validators must not leave callbacks that cannot
        # be reconstructed when the accepted Update replays without validation.
        ThreadLocalContext.get&.send(:check_update_validator!, 'register Future callbacks')
        execution = Thread.current[UpdateExecution::CURRENT_KEY]
        execution ? execution.register_callback(block) : block
      end
    end
  end
end

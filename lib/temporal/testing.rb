require 'temporal/testing/temporal_override'
require 'temporal/testing/workflow_override'
require 'temporal/testing/scheduled_workflows'
require 'temporal/testing/deferred_starts'

module Temporal
  module Testing
    DISABLED_MODE = nil
    LOCAL_MODE = :local

    class << self
      def local!(&block)
        set_mode(LOCAL_MODE, &block)
      end

      def disabled!(&block)
        set_mode(DISABLED_MODE, &block)
      end

      def disabled?
        mode == DISABLED_MODE
      end

      def local?
        mode == LOCAL_MODE
      end

      # True when start_workflow calls should be deferred rather than run inline.
      # Only meaningful in local mode.
      def defer_starts?
        local? && @defer_starts == true
      end

      # Within the block, fire-and-forget start_workflow calls are deferred instead
      # of run inline; the queued workflows run when the block exits normally (after
      # the caller's stack -- and any locks it held -- have unwound). This models the
      # async ordering of a real Temporal server in local mode. See DeferredStarts.
      #
      # Requires local! mode and cannot be nested.
      def with_deferred_starts
        raise 'Temporal::Testing.with_deferred_starts requires Temporal::Testing.local!' unless local?

        # The queue is a single flat array, so a nested block would drain and clear
        # the outer block's queued starts. Forbid nesting rather than drop workflows
        # silently.
        raise 'Temporal::Testing.with_deferred_starts cannot be nested' if defer_starts?

        @defer_starts = true
        begin
          result = yield
          # Clear the flag before draining so the queued workflows run inline (and
          # could, in turn, start and await their own children).
          @defer_starts = false
          DeferredStarts.execute_all
          result
        ensure
          @defer_starts = false
          DeferredStarts.clear_all
        end
      end

      private

      attr_reader :mode

      def set_mode(new_mode, &block)
        if block_given?
          with_mode(new_mode, &block)
        else
          @mode = new_mode
        end
      end

      def with_mode(new_mode, &block)
        previous_mode = mode
        @mode = new_mode
        yield
      ensure
        @mode = previous_mode
      end
    end
  end
end

Temporal::Client.prepend Temporal::Testing::TemporalOverride
Temporal::Workflow.extend Temporal::Testing::WorkflowOverride

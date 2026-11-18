require 'fiber'
require 'temporal/thread_local_context'

module Temporal
  class Workflow
    # One accepted Update and the Future callbacks registered by its fibers.
    # Callback ownership is captured at registration, even for a Future created
    # by the main workflow or another Update. Completion drains that owned work.
    class UpdateExecution
      CURRENT_KEY = :temporal_update_execution

      class Callback
        def initialize(execution, block)
          @execution, @block = execution, block
          @finished = false
        end

        def call(*args)
          return if @finished
          begin
            @execution.with_current { @block.call(*args) }
          rescue StandardError, ScriptError => error
            @execution.callback_failed(error)
          ensure
            discard
          end
        end

        # A success callback is discarded on failure, and vice versa. It must
        # stop holding the Update open even though its block will never run.
        def discard
          return if @finished
          @finished = true
          @execution.callback_finished(self)
        end
      end

      def initialize(context, info, &complete)
        @context, @info, @complete = context, info, complete
        @callbacks = {}
        @handler_finished = @completed = false
      end

      def start(&handler)
        @fiber = Fiber.new do
          with_current do
            begin
              @result = handler.call
            rescue StandardError, ScriptError => error
              @error = error
            end
            @handler_finished = true
            complete_if_finished
          end
        end
        @fiber.resume
      end

      def register_callback(block)
        callback = Callback.new(self, block)
        @callbacks[callback] = true
        callback
      end

      def with_current
        previous_context = ThreadLocalContext.get
        previous_update = Thread.current[:temporal_current_update]
        previous_execution = Thread.current[CURRENT_KEY]
        ThreadLocalContext.set(@context)
        Thread.current[:temporal_current_update] = @info
        Thread.current[CURRENT_KEY] = self
        yield
      ensure
        Thread.current[CURRENT_KEY] = previous_execution
        Thread.current[:temporal_current_update] = previous_update
        ThreadLocalContext.set(previous_context)
      end

      def callback_failed(error)
        if Fiber.current == @fiber
          # Already-completed Futures invoke their callbacks inline.
          raise error
        elsif !@handler_finished && @fiber.alive?
          # Deliver to the suspended handler's rescue/ensure blocks. Merely
          # raising in the separate callback fiber would fail the workflow task
          # forever, leaving the accepted Update without an outcome.
          @fiber.raise(error)
        else
          # The handler returned while an owned callback was still pending.
          @error ||= error
        end
      end

      def callback_finished(callback)
        @callbacks.delete(callback)
        complete_if_finished
      end

      private

      def complete_if_finished
        return unless @handler_finished && @callbacks.empty? && !@completed
        @completed = true
        @complete.call(@result, @error)
      end
    end
  end
end

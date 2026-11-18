require 'google/protobuf/well_known_types'
require 'temporal/connection/serializer/failure'
require 'temporal/workflow/update_execution'

module Temporal
  class Workflow
    # The StateManager owns the durable commands; this registry owns the Ruby
    # handler fibers. Both are rebuilt from full history for every workflow task.
    class UpdateRegistry
      Handler = Struct.new(:block, :validator)
      attr_reader :active_count

      def initialize(state_manager, dispatcher, converter)
        @state_manager, @dispatcher, @converter = state_manager, dispatcher, converter
        @handlers, @processed = {}, {}
        @active_count = 0
        dispatcher.register_handler(History::EventTarget.update_request, 'update_requested') do |message, replay|
          process(message, replay)
        end
      end

      def register(name, context, validator: nil, &block)
        raise ArgumentError, 'An Update handler block is required' unless block
        raise ArgumentError, 'Update name must not be empty' if name.to_s.empty?
        raise ArgumentError, "Update handler already registered: #{name}" if @handlers.key?(name.to_s)
        @context = context
        @handlers[name.to_s] = Handler.new(block, validator)
      end

      def registered?
        !@handlers.empty?
      end

      def validating?
        @validating == true
      end

      private

      def process(message, replay)
        unless message.body&.type_url&.end_with?('/temporal.api.update.v1.Request')
          raise Temporal::InternalError, "Unsupported workflow protocol message: #{message.body&.type_url}"
        end
        return if @processed[message.protocol_instance_id]
        @processed[message.protocol_instance_id] = true
        request = Temporalio::Api::Update::V1::Request.decode(message.body.value)
        unless request.meta && request.input && request.meta.update_id == message.protocol_instance_id
          raise Temporal::InternalError, 'Malformed Update request or mismatched Update ID'
        end
        handler = @handlers[request.input.name]
        begin
          raise ArgumentError, "Unknown Update handler: #{request.input.name}" unless handler
          input = @converter.from_payloads(request.input.args) || []
          headers = @converter.from_payload_map(request.input.header&.fields || {})
          validate(handler, input) unless replay
        rescue StandardError, ScriptError => e
          # Accepted requests must replay their handlers; a new validator must
          # never reject an Update that was already durably accepted.
          raise Temporal::NonDeterministicWorkflowError, "Cannot replay accepted Update: #{e.message}" if replay
          emit(message, 'reject', Temporalio::Api::Update::V1::Rejection.new(
            rejected_request_message_id: message.id, rejected_request_sequencing_event_id: message.event_id,
            rejected_request: request, failure: failure(e)
          ), durable: false)
          return
        end

        emit(message, 'accept', Temporalio::Api::Update::V1::Acceptance.new(
          accepted_request_message_id: message.id, accepted_request_sequencing_event_id: message.event_id,
          accepted_request: request
        ))
        @active_count += 1
        info = {
          id: request.meta.update_id, name: request.input.name,
          headers: headers
        }.freeze
        execution = UpdateExecution.new(@context, info) do |result, error|
          begin
            outcome = if error
                        Temporalio::Api::Update::V1::Outcome.new(failure: failure(error))
                      else
                        Temporalio::Api::Update::V1::Outcome.new(success: @converter.to_result_payloads(result))
                      end
          rescue StandardError, ScriptError => e
            outcome = Temporalio::Api::Update::V1::Outcome.new(failure: failure(e))
          end
          emit(message, 'complete', Temporalio::Api::Update::V1::Response.new(meta: request.meta, outcome: outcome))
          @active_count -= 1
          @dispatcher.dispatch(History::EventTarget.update_request, 'update_completed')
        end
        execution.start { handler.block.call(*input) }
      end

      def validate(handler, input)
        return unless handler.validator
        @validating = true
        fiber = Fiber.new do
          ThreadLocalContext.set(@context)
          handler.validator.call(*input)
        ensure
          ThreadLocalContext.set(nil)
        end
        fiber.resume
        raise ArgumentError, 'Update validators must not yield' if fiber.alive?
      ensure
        @validating = false
      end

      def failure(error)
        Temporal::Connection::Serializer::Failure.new(error, @converter).to_proto
      end

      def emit(request_message, suffix, body, durable: true)
        message = Temporalio::Api::Protocol::V1::Message.new(
          id: "#{request_message.protocol_instance_id}/#{suffix}",
          protocol_instance_id: request_message.protocol_instance_id,
          body: Google::Protobuf::Any.pack(body)
        )
        @state_manager.emit_update_message(message, durable: durable)
      end
    end
  end
end

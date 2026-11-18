require 'temporal/connection/serializer/base'
require 'temporal/errors'
require 'google/protobuf/duration_pb'

module Temporal
  module Connection
    module Serializer
      class Failure < Base
        def initialize(error, converter, serialize_whole_error: false, max_bytes: 200_000)
          @serialize_whole_error = serialize_whole_error
          @max_bytes = max_bytes
          super(error, converter)
        end

        def to_proto
          if @serialize_whole_error
            details = converter.to_details_payloads(object)
            if details.payloads.first.data.size > @max_bytes
              Temporal.logger.error(
                "Could not serialize exception because it's too large, so we are using a fallback that may not "\
                  "deserialize correctly on the client.  First #{@max_bytes} bytes:\n" \
                "#{details.payloads.first.data[0..@max_bytes - 1]}",
                {unserializable_error: object.class.name}
              )
              # Fallback to a more conservative serialization if the payload is too big to avoid
              # sending a huge amount of data to temporal and putting it in the history.
              details = converter.to_details_payloads(object.message)
            end
          else
            details = converter.to_details_payloads(object.message)
          end
          next_retry_delay = duration_from(object.next_retry_delay) if object.is_a?(Temporal::RetryDelay)
          Temporalio::Api::Failure::V1::Failure.new(
            message: object.message,
            stack_trace: stack_trace_from(object.backtrace),
            application_failure_info: Temporalio::Api::Failure::V1::ApplicationFailureInfo.new(
              type: object.class.name,
              details: details,
              next_retry_delay: next_retry_delay
            )
          )
        end

        private

        def duration_from(value)
          # A failure read from history can carry a delay rejected by this server. Keep its error intact on re-raise.
          return unless Temporal::RetryDelay.valid_seconds?(value)

          nanoseconds = (value.to_r * 1_000_000_000).round
          seconds, nanos = nanoseconds.divmod(1_000_000_000)
          Google::Protobuf::Duration.new(seconds: seconds, nanos: nanos)
        end

        def stack_trace_from(backtrace)
          return unless backtrace

          backtrace.join("\n")
        end
      end
    end
  end
end

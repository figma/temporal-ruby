require 'temporal/connection/serializer/base'

module Temporal
  module Connection
    module Serializer
      class ProtocolMessage < Base
        def to_proto
          Temporalio::Api::Command::V1::Command.new(
            command_type: :COMMAND_TYPE_PROTOCOL_MESSAGE,
            protocol_message_command_attributes: Temporalio::Api::Command::V1::ProtocolMessageCommandAttributes.new(
              message_id: object.message.id
            )
          )
        end
      end
    end
  end
end

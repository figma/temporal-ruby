module Temporal
  class Workflow
    class History
      # Accepted requests are carried in history and re-delivered at their
      # original sequencing event, before commands from the accepting task.
      class UpdateRequest
        attr_reader :message, :timestamp, :replay

        def initialize(message, timestamp, replay:)
          @message, @timestamp, @replay = message, timestamp, replay
        end

        def id
          message.event_id
        end

        def type
          'WORKFLOW_UPDATE_REQUEST'
        end
      end
    end
  end
end

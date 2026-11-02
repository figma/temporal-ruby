require 'temporal/workflow/history/event'
require 'temporal/workflow/history/window'
require 'temporal/workflow/history/update_request'
require 'google/protobuf/well_known_types'

module Temporal
  class Workflow
    class History
      attr_reader :events

      def initialize(events, messages: [])
        @events = events.map { |event| History::Event.new(event) }
        pending = {}
        admitted = @events.each_with_object({}) do |event, requests|
          requests[event.id] = event.attributes.request if event.type == 'WORKFLOW_EXECUTION_UPDATE_ADMITTED'
        end
        @events.each do |event|
          next unless event.type == 'WORKFLOW_EXECUTION_UPDATE_ACCEPTED'
          attrs = event.attributes
          request = attrs.accepted_request || admitted[attrs.accepted_request_sequencing_event_id]
          raise Temporal::InternalError, 'Accepted Update request is missing from history' unless request
          message = Temporalio::Api::Protocol::V1::Message.new(
            id: attrs.accepted_request_message_id, protocol_instance_id: attrs.protocol_instance_id,
            event_id: attrs.accepted_request_sequencing_event_id,
            body: Google::Protobuf::Any.pack(request)
          )
          pending[message.protocol_instance_id] = [message, true]
        end
        @events.each do |event|
          next unless event.type == 'WORKFLOW_EXECUTION_UPDATE_ADMITTED'
          request = event.attributes.request
          id = request.meta.update_id
          message = Temporalio::Api::Protocol::V1::Message.new(
            id: "#{id}/request", protocol_instance_id: id, event_id: event.id,
            body: Google::Protobuf::Any.pack(request)
          )
          # If this request was subsequently accepted, use its recorded message
          # identity/sequence above and skip validation. Otherwise admission is
          # a new delivery, including on a reset branch, and must be validated.
          pending[id] ||= [message, false]
        end
        messages.each { |message| pending[message.protocol_instance_id] ||= [message, false] }
        sequenced = pending.values.group_by { |message, _| message.event_id }
        stream = @events.flat_map do |event|
          [event] + (sequenced.delete(event.id) || []).map do |message, replay|
            History::UpdateRequest.new(message, event.timestamp, replay: replay)
          end
        end
        raise Temporal::InternalError, 'Update sequencing event is missing from history' unless sequenced.empty?
        @iterator = stream.each
      end

      def find_event_by_id(id)
        events.find { |event| event.id == id }
      end

      # It is very important to replay the History window by window in order to
      # simulate the exact same state the workflow was in when it processed the
      # workflow task for the first time.
      #
      # A history window consists of 3 parts:
      #
      # 1. Events that happened since the last window (timer fired, activity completed, etc)
      # 2. A workflow task related events (workflow task started, completed, failed, etc)
      # 3. Commands issued by the last workflow task (^) (schedule activity, start timer, etc)
      #
      def next_window
        return unless peek_event

        window = History::Window.new

        while event = next_event
          window.add(event)

          break if event.type == 'WORKFLOW_TASK_COMPLETED'
        end

        # Find the end of the window by exhausting all the commands
        window.add(next_event) while command?(peek_event)

        window.freeze
      end

      private

      COMMAND_EVENT_TYPES = %w[
        WORKFLOW_EXECUTION_UPDATE_ACCEPTED
        WORKFLOW_EXECUTION_UPDATE_COMPLETED
        ACTIVITY_TASK_SCHEDULED
        ACTIVITY_TASK_CANCEL_REQUESTED
        TIMER_STARTED
        CANCEL_TIMER_FAILED
        TIMER_CANCELED
        WORKFLOW_EXECUTION_CANCEL_REQUESTED
        WORKFLOW_EXECUTION_COMPLETED
        WORKFLOW_EXECUTION_CONTINUED_AS_NEW
        WORKFLOW_EXECUTION_FAILED
        START_CHILD_WORKFLOW_EXECUTION_INITIATED
        SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED
        REQUEST_CANCEL_ACTIVITY_TASK_FAILED
        REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED
        MARKER_RECORDED
        UPSERT_WORKFLOW_SEARCH_ATTRIBUTES
      ].freeze

      attr_reader :iterator

      def next_event
        iterator.next rescue nil
      end

      def peek_event
        iterator.peek rescue nil
      end

      def command?(event)
        COMMAND_EVENT_TYPES.include?(event&.type)
      end
    end
  end
end

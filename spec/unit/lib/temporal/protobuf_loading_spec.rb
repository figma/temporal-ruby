require 'open3'
require 'rbconfig'

describe 'Temporal protobuf loading' do
  let(:lib_dir) { File.expand_path('../../../../lib', __dir__) }

  def run_probe(code, disable_proto_load: nil)
    Open3.capture3(
      { 'COINBASE_TEMPORAL_RUBY_DISABLE_PROTO_LOAD' => disable_proto_load },
      RbConfig.ruby, '-I', lib_dir, '-e', code
    )
  end

  let(:exercise_coinbase) do
    <<~'RUBY'
      require 'temporal-ruby'
      require 'temporal/worker'
      require 'temporal/testing/replay_tester'

      payload_converter = Temporal::Connection::Converter::Payload::ProtoJSON.new
      execution = Temporalio::Api::Common::V1::WorkflowExecution.new(workflow_id: 'test-id')
      payload = payload_converter.to_payload(execution)
      raise 'protobuf payload did not round trip' unless payload_converter.from_payload(payload) == execution

      converter = Temporal::ConverterWrapper.new(
        Temporal::Configuration::DEFAULT_CONVERTER,
        Temporal::Configuration::DEFAULT_PAYLOAD_CODEC
      )
      command = Temporal::Workflow::Command::CompleteWorkflow.new(result: 'test-result')
      proto = Temporal::Connection::Serializer.serialize(command, converter)
      raise 'wrong command type' unless proto.command_type == :COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION
      result = converter.from_result_payloads(proto.complete_workflow_execution_command_attributes.result)
      raise 'result did not round trip' unless result == 'test-result'

      workflow_stub = Temporalio::Api::WorkflowService::V1::WorkflowService::Stub
      operator_stub = Temporalio::Api::OperatorService::V1::OperatorService::Stub
      raise 'missing Ruby gRPC stubs' unless workflow_stub < GRPC::ClientStub && operator_stub < GRPC::ClientStub
    RUBY
  end

  it 'loads its own messages and stubs without the opt-in' do
    stdout, stderr, status = run_probe(exercise_coinbase + <<~'RUBY')
      raise 'legacy messages were not loaded' unless $LOADED_FEATURES.any? do |path|
        path.include?('/gen/temporal/api/common/v1/message_pb.rb')
      end
    RUBY
    expect(status.success?).to eq(true), "#{stdout}\n#{stderr}"
  end

  it 'uses official messages with Coinbase serializers, worker definitions, and gRPC stubs' do
    skip 'Run with BUNDLE_GEMFILE=gemfiles/temporalio.gemfile' unless ENV['BUNDLE_GEMFILE']&.end_with?('temporalio.gemfile')

    stdout, stderr, status = run_probe(
      "require 'temporalio/client'\nrequire 'temporalio/worker'\n" + exercise_coinbase + <<~'RUBY',
        message_loads = $LOADED_FEATURES.grep(%r{/gen/temporal/api/.*_pb\.rb$}).reject do |path|
          path.end_with?('_services_pb.rb')
        end
        raise "Coinbase messages loaded: #{message_loads}" unless message_loads.empty?
        raise 'Coinbase gen directory entered the load path' if $LOAD_PATH.any? { |path| path.end_with?('/lib/gen') }
        raise 'official client not loaded' unless defined?(Temporalio::Client)
        raise 'official worker not loaded' unless defined?(Temporalio::Worker)
      RUBY
      disable_proto_load: '1'
    )
    expect(status.success?).to eq(true), "#{stdout}\n#{stderr}"
  end
end

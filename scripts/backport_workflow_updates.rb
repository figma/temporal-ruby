# frozen_string_literal: true

# Additive backport from temporalio/api v1.44.0 onto the pinned proto submodule.
# Source: https://github.com/temporalio/api/tree/v1.44.0/temporal/api
# Run by `make proto` after backport_workflow_id_conflict_policy.rb.
# Patching descriptors retains the existing API surface and avoids upgrading all
# generated dependencies. Field numbers, types and service names match upstream.
require 'google/protobuf/descriptor_pb'

ROOT = File.expand_path('../lib/gen/temporal/api', __dir__)
P = Google::Protobuf
SERVICE = '.temporal.api.workflowservice.v1.'

def field(name, number, type, type_name = nil, **options)
  P::FieldDescriptorProto.new(name: name, number: number, type: type,
                             label: :LABEL_OPTIONAL, **(type_name ? { type_name: type_name } : {}), **options)
end

def add_field(message, value)
  existing = message.field.find { |f| f.number == value.number || f.name == value.name }
  raise "Conflicting protobuf field #{value.name}" if existing && existing != value
  message.field << value unless existing
end

def patch(path, constants = {})
  filename = File.join(ROOT, path)
  source = File.read(filename)
  line = source.lines.find { |l| l.start_with?('descriptor_data = ') }
  # Evaluate only protoc's string literal, never the generated file.
  descriptor = P::FileDescriptorProto.decode(eval(line.delete_prefix('descriptor_data = ')))
  yield descriptor
  source = source.sub(line) { "descriptor_data = #{P::FileDescriptorProto.encode(descriptor).dump}\n" }
  constants.each do |name, (full_name, kind)|
    next if source.include?("#{name} = ")
    insertion = "        #{name} = ::Google::Protobuf::DescriptorPool.generated_pool.lookup(#{full_name.inspect}).#{kind}\n"
    source = source.sub(/(      module V1\n)/, "\\1#{insertion}")
  end
  File.write(filename, source)
end

constants = {}
%w[Request Response].each do |suffix|
  name = "ExecuteMultiOperation#{suffix}"
  constants[name] = [SERVICE.delete_prefix('.') + name, 'msgclass']
  child = suffix == 'Request' ? 'Operation' : 'Response'
  constants["#{name}::#{child}"] = [SERVICE.delete_prefix('.') + "#{name}.#{child}", 'msgclass']
end
# Parent constants must precede nested constants.
constants = constants.sort_by { |name, _| name.include?('::') ? 0 : 1 }.to_h
patch('workflowservice/v1/request_response_pb.rb', constants) do |d|
  get = ->(name) { d.message_type.find { |m| m.name == name } }
  add_field(get.call('StartWorkflowExecutionResponse'), field('started', 3, :TYPE_BOOL))
  add_field(get.call('UpdateWorkflowExecutionResponse'), field('stage', 3, :TYPE_ENUM, '.temporal.api.enums.v1.UpdateWorkflowExecutionLifecycleStage'))
  add_field(get.call('PollWorkflowExecutionUpdateResponse'), field('stage', 2, :TYPE_ENUM, '.temporal.api.enums.v1.UpdateWorkflowExecutionLifecycleStage'))
  add_field(get.call('PollWorkflowExecutionUpdateResponse'), field('update_ref', 3, :TYPE_MESSAGE, '.temporal.api.update.v1.UpdateRef'))
  dependency = 'temporal/api/enums/v1/update.proto'
  d.dependency << dependency unless d.dependency.include?(dependency)
  %w[Request Response].each do |suffix|
    name = "ExecuteMultiOperation#{suffix}"
    next if get.call(name)
    child = suffix == 'Request' ? 'Operation' : 'Response'
    nested = P::DescriptorProto.new(name: child, oneof_decl: [P::OneofDescriptorProto.new(name: child.downcase)], field: [
      field('start_workflow', 1, :TYPE_MESSAGE, SERVICE + "StartWorkflowExecution#{suffix}", oneof_index: 0),
      field('update_workflow', 2, :TYPE_MESSAGE, SERVICE + "UpdateWorkflowExecution#{suffix}", oneof_index: 0)
    ])
    fields = suffix == 'Request' ? [field('namespace', 1, :TYPE_STRING), field('operations', 2, :TYPE_MESSAGE, SERVICE + name + '.' + child, label: :LABEL_REPEATED)] :
      [field('responses', 1, :TYPE_MESSAGE, SERVICE + name + '.' + child, label: :LABEL_REPEATED)]
    d.message_type << P::DescriptorProto.new(name: name, field: fields, nested_type: [nested])
  end
end
patch('workflowservice/v1/service_pb.rb') do |d|
  service = d.service.find { |s| s.name == 'WorkflowService' }
  unless service['method'].any? { |m| m.name == 'ExecuteMultiOperation' }
    service['method'] << P::MethodDescriptorProto.new(name: 'ExecuteMultiOperation', input_type: SERVICE + 'ExecuteMultiOperationRequest', output_type: SERVICE + 'ExecuteMultiOperationResponse')
  end
end
path = File.join(ROOT, 'workflowservice/v1/service_services_pb.rb')
source = File.read(path)
unless source.include?('rpc :ExecuteMultiOperation,')
  source = source.sub('            rpc :UpdateWorkflowExecution,', "            rpc :ExecuteMultiOperation, ::Temporalio::Api::WorkflowService::V1::ExecuteMultiOperationRequest, ::Temporalio::Api::WorkflowService::V1::ExecuteMultiOperationResponse\n            rpc :UpdateWorkflowExecution,")
end
File.write(path, source)

# Resets can reapply an accepted Update as a durable admitted request.
patch('enums/v1/update_pb.rb', 'UpdateAdmittedEventOrigin' => ['temporal.api.enums.v1.UpdateAdmittedEventOrigin', 'enummodule']) do |d|
  unless d.enum_type.any? { |e| e.name == 'UpdateAdmittedEventOrigin' }
    d.enum_type << P::EnumDescriptorProto.new(name: 'UpdateAdmittedEventOrigin', value: [
      P::EnumValueDescriptorProto.new(name: 'UPDATE_ADMITTED_EVENT_ORIGIN_UNSPECIFIED', number: 0),
      P::EnumValueDescriptorProto.new(name: 'UPDATE_ADMITTED_EVENT_ORIGIN_REAPPLY', number: 1)
    ])
  end
end
patch('enums/v1/event_type_pb.rb') do |d|
  values = d.enum_type.find { |e| e.name == 'EventType' }.value
  unless values.any? { |v| v.name == 'EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED' }
    values << P::EnumValueDescriptorProto.new(name: 'EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED', number: 47)
  end
end
patch('history/v1/message_pb.rb', 'WorkflowExecutionUpdateAdmittedEventAttributes' => ['temporal.api.history.v1.WorkflowExecutionUpdateAdmittedEventAttributes', 'msgclass']) do |d|
  name = 'WorkflowExecutionUpdateAdmittedEventAttributes'
  unless d.message_type.any? { |m| m.name == name }
    d.message_type << P::DescriptorProto.new(name: name, field: [
      field('request', 1, :TYPE_MESSAGE, '.temporal.api.update.v1.Request'),
      field('origin', 2, :TYPE_ENUM, '.temporal.api.enums.v1.UpdateAdmittedEventOrigin')
    ])
  end
  dependency = 'temporal/api/enums/v1/update.proto'
  d.dependency << dependency unless d.dependency.include?(dependency)
  add_field(d.message_type.find { |m| m.name == 'HistoryEvent' },
            field('workflow_execution_update_admitted_event_attributes', 52, :TYPE_MESSAGE,
                  '.temporal.api.history.v1.' + name, oneof_index: 0))
  add_field(d.message_type.find { |m| m.name == 'WorkflowExecutionUpdateCompletedEventAttributes' },
            field('accepted_event_id', 3, :TYPE_INT64))
end

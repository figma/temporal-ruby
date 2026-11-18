# The checked-in Temporal API bindings predate the workflow ID conflict policy.
# Apply only the additive descriptor changes needed for ordinary starts.

proto_out = ARGV[0] || File.expand_path('../lib/gen', __dir__)
enum_path = File.join(proto_out, 'temporal/api/enums/v1/workflow_pb.rb')
request_path = File.join(proto_out, 'temporal/api/workflowservice/v1/request_response_pb.rb')

def replace_once(path, before, after, marker)
  source = File.read(path)
  return if source.include?(marker)

  raise "Unexpected generated binding: #{path}" unless source.scan(before).length == 1

  File.write(path, source.sub(before, after))
end

enum_field = '*\\xcf\\x01\\n\\x18WorkflowIdConflictPolicy' \
  "\\x12+\\n'WORKFLOW_ID_CONFLICT_POLICY_UNSPECIFIED\\x10\\x00" \
  "\\x12$\\n WORKFLOW_ID_CONFLICT_POLICY_FAIL\\x10\\x01" \
  "\\x12,\\n(WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING\\x10\\x02" \
  "\\x122\\n.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING\\x10\\x03"

replace_once(
  enum_path,
  'HEARTBEAT\\x10\\x04\\x42\\x85',
  "HEARTBEAT\\x10\\x04#{enum_field}B\\x85",
  '*\\xcf\\x01\\n\\x18WorkflowIdConflictPolicy'
)
replace_once(
  enum_path,
  '        WorkflowIdReusePolicy =',
  '        WorkflowIdConflictPolicy = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("temporal.api.enums.v1.WorkflowIdConflictPolicy").enummodule' + "\n" + '        WorkflowIdReusePolicy =',
  'WorkflowIdConflictPolicy ='
)

request_source = File.read(request_path)
unless request_source.include?('workflow_id_conflict_policy')
  request_source.sub!('\\"\\xfb\\x07\\n\\x1dStartWorkflowExecutionRequest', '\\"\\xd1\\x08\\n\\x1dStartWorkflowExecutionRequest') or raise 'Unexpected StartWorkflowExecutionRequest length'
  request_source.sub!(
    'workflow_start_delay\\x18\\x14 \\x01(\\x0b\\x32\\x19.google.protobuf.DurationB\\x04\\x98\\xdf\\x1f\\x01\\"',
    'workflow_start_delay\\x18\\x14 \\x01(\\x0b\\x32\\x19.google.protobuf.DurationB\\x04\\x98\\xdf\\x1f\\x01\\x12T\\n\\x1bworkflow_id_conflict_policy\\x18\\x16 \\x01(\\x0e\\x32/.temporal.api.enums.v1.WorkflowIdConflictPolicy\\"'
  ) or raise 'Unexpected StartWorkflowExecutionRequest fields'
  File.write(request_path, request_source)
end

# Inspect the descriptor itself: another backport may already provide the field
# and may render the generated Ruby string literal with different escapes.
require 'google/protobuf/descriptor_pb'
source = File.read(request_path)
line = source.lines.find { |l| l.start_with?('descriptor_data = ') } or raise 'Missing descriptor'
descriptor = Google::Protobuf::FileDescriptorProto.decode(eval(line.delete_prefix('descriptor_data = ')))
response = descriptor.message_type.find { |msg| msg.name == 'StartWorkflowExecutionResponse' }
started = Google::Protobuf::FieldDescriptorProto.new(name: 'started', number: 3, type: :TYPE_BOOL, label: :LABEL_OPTIONAL)
existing = response.field.find { |f| f.number == 3 || f.name == 'started' }
raise 'Conflicting StartWorkflowExecutionResponse.started field' if existing && existing != started
unless existing
  response.field << started
  encoded = Google::Protobuf::FileDescriptorProto.encode(descriptor)
  File.write(request_path, source.sub(line) { "descriptor_data = #{encoded.dump}\n" })
end

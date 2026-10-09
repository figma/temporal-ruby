#!/usr/bin/env ruby
# Restore the additive next_retry_delay field after regenerating the pinned API proto.

require 'google/protobuf/descriptor_pb'

path = File.expand_path('../lib/gen/temporal/api/failure/v1/message_pb.rb', __dir__)
source = File.read(path)
match = source.match(/^descriptor_data = (".*")$/)
raise 'Unexpected generated failure descriptor' unless match

descriptor = Google::Protobuf::FileDescriptorProto.decode(match[1].undump)
raise 'Unexpected failure proto' unless descriptor.name == 'temporal/api/failure/v1/message.proto'
application = descriptor.message_type.find { |message| message.name == 'ApplicationFailureInfo' }
raise 'Missing ApplicationFailureInfo' unless application
field = application.field.find { |item| item.name == 'next_retry_delay' }
if field
  raise 'Unexpected next_retry_delay field' unless field.number == 4 && field.type_name == '.google.protobuf.Duration'
  exit 0
end
raise 'Field 4 is occupied' if application.field.any? { |item| item.number == 4 }
raise 'Duration already imported' if descriptor.dependency.include?('google/protobuf/duration.proto')

application.field << Google::Protobuf::FieldDescriptorProto.new(
  name: 'next_retry_delay', number: 4,
  label: :LABEL_OPTIONAL, type: :TYPE_MESSAGE, type_name: '.google.protobuf.Duration'
)
descriptor.dependency << 'google/protobuf/duration.proto'
source.sub!(match[0]) { "descriptor_data = #{Google::Protobuf::FileDescriptorProto.encode(descriptor).dump}" }
old_require = "require 'temporal/api/enums/v1/workflow_pb'"
raise 'Unexpected generated failure imports' unless source.scan(old_require).size == 1
source.sub!(old_require, "#{old_require}\nrequire 'google/protobuf/duration_pb'")
File.write(path, source)

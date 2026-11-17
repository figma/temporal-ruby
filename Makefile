PROTO_ROOT := proto
PROTO_FILES = $(shell find $(PROTO_ROOT) -name "*.proto")
PROTO_DIRS = $(sort $(dir $(PROTO_FILES)))
PROTO_OUT := lib/gen

proto:
	$(foreach PROTO_DIR,$(PROTO_DIRS),bundle exec grpc_tools_ruby_protoc -Iproto --ruby_out=$(PROTO_OUT) --grpc_out=$(PROTO_OUT) $(PROTO_DIR)*.proto;)
	sed -i "/^require 'temporal\// s/$$/ unless ENV['COINBASE_TEMPORAL_RUBY_DISABLE_PROTO_LOAD'] == '1'/" $(PROTO_OUT)/temporal/api/workflowservice/v1/service_services_pb.rb $(PROTO_OUT)/temporal/api/operatorservice/v1/service_services_pb.rb

.PHONY: proto

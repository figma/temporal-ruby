PROTO_ROOT := proto
PROTO_FILES = $(shell find $(PROTO_ROOT) -name "*.proto")
PROTO_DIRS = $(sort $(dir $(PROTO_FILES)))
PROTO_OUT := lib/gen

proto:
	@set -e; $(foreach PROTO_DIR,$(PROTO_DIRS),bundle exec grpc_tools_ruby_protoc -Iproto --ruby_out=$(PROTO_OUT) --grpc_out=$(PROTO_OUT) $(PROTO_DIR)*.proto;)
	@for script in scripts/backport_*.rb; do bundle exec ruby "$$script" || exit $$?; done

.PHONY: proto

_Written with AI_

# Load both Ruby SDKs in one process

The two SDKs define the same Temporal API protobuf messages. Load the official
messages first. Then disable this SDK's message loading before requiring it:

```ruby
require 'temporalio/client'
ENV['COINBASE_TEMPORAL_RUBY_DISABLE_PROTO_LOAD'] = '1'
require 'temporal-ruby'
```

The option is process-wide. Set it before the first Coinbase require. Do not
change it after boot. The Coinbase gRPC service stubs still load, but use the
official SDK's message classes. Without the option, Coinbase loading stays
unchanged. The `temporalio` gem is not a runtime dependency of this gem.

This follows the approach in [Coinbase PR 335](https://github.com/coinbase/temporal-ruby/pull/335).
It does not make the two Workflow runtimes' histories compatible. Each Task
Queue must have only one Workflow runtime, even if a process loads both SDKs.
Do not create official runtimes or clients before a process forks.

Run the protobuf loading and serialization probes with:

```sh
BUNDLE_GEMFILE=gemfiles/temporalio.gemfile bundle install
BUNDLE_GEMFILE=gemfiles/temporalio.gemfile bundle exec rspec spec/unit/lib/temporal/protobuf_loading_spec.rb
```

These probes do not connect to Temporal. Application boot, fork, polling, and
payload compatibility need separate environment tests.

# Ruby SDK for Temporal

[![Coverage Status](https://coveralls.io/repos/github/coinbase/temporal-ruby/badge.svg?branch=master)](https://coveralls.io/github/coinbase/temporal-ruby?branch=master)

<img src="./assets/temporal_logo.png" width="250" align="right" alt="Temporal" />

A pure Ruby library for defining and running Temporal workflows and activities.

To find more about Temporal itself please visit <https://temporal.io/>.


## Getting Started

Clone this repository:

```sh
git clone git@github.com:coinbase/temporal-ruby.git
```

Include this gem to your `Gemfile`:

```ruby
gem 'temporal-ruby', github: 'coinbase/temporal-ruby'
```

Define an activity:

```ruby
require 'temporal-ruby'
class HelloActivity < Temporal::Activity
  def execute(name)
    puts "Hello #{name}!"

    return nil
  end
end
```

Define a workflow:

```ruby
require 'path/to/hello_activity'

class HelloWorldWorkflow < Temporal::Workflow
  def execute
    HelloActivity.execute!('World')

    return nil
  end
end
```

Configure your Temporal connection and register the namespace with the Temporal service:

```ruby
require 'temporal-ruby'
Temporal.configure do |config|
  config.host = 'localhost'
  config.port = 7233
  config.namespace = 'ruby-samples'
  config.task_queue = 'hello-world'
  config.credentials = :this_channel_is_insecure
end

begin
  Temporal.register_namespace('ruby-samples', 'A safe space for playing with Temporal Ruby')
rescue Temporal::NamespaceAlreadyExistsFailure
  nil # service was already registered
end
```


Configure and start your worker process in a terminal shell:

```ruby
require 'path/to/configuration'
require 'temporal/worker'

worker = Temporal::Worker.new
worker.register_workflow(HelloWorldWorkflow)
worker.register_activity(HelloActivity)
worker.start # runs forever
```

You can add several options when initializing worker (here defaults are provided as values):

```ruby
Temporal::Worker.new(
  activity_thread_pool_size: 20, # how many threads poll for activities
  workflow_thread_pool_size: 10, # how many threads poll for workflows
  binary_checksum: nil, # identifies the version of workflow worker code
  activity_poll_retry_seconds: 0, # how many seconds to wait after unsuccessful poll for activities
  workflow_poll_retry_seconds: 0, # how many seconds to wait after unsuccessful poll for workflows
  activity_max_tasks_per_second: 0 # rate-limit for starting activity tasks (new activities + retries) on the task queue
)
```

And finally start your workflow in another terminal shell:

```ruby
require 'path/to/configuration'
require 'path/to/hello_world_workflow'

Temporal.start_workflow(HelloWorldWorkflow)
```

Congratulation you've just created and executed a distributed workflow!

To view more details about your execution, point your browser to
<http://localhost:8088/namespace/ruby-samples/workflows?range=last-3-hours&status=CLOSED>.

There are plenty of [runnable examples](examples/) demonstrating various features of this library
available, make sure to check them out.


## Installing dependencies

Temporal service handles all the persistence, fault tolerance and coordination of your workflows and
activities. To set it up locally, download and boot the Docker Compose file from the official repo.
The Docker Compose file forwards all ports to your localhost so you can interact with
the containers easily from your shells.

Run:

```sh
curl -O https://raw.githubusercontent.com/temporalio/docker-compose/main/docker-compose.yml

docker-compose up
```

## Using Credentials

### SSL

In many production deployments you will end up connecting to your Temporal Services via SSL. In this
case you must read the public certificate of the CA that issued your Temporal server's SSL certificate and create
an instance of [gRPC Channel Credentials](https://grpc.io/docs/guides/auth/#with-server-authentication-ssltls-1).

Configure your Temporal connection:

```ruby
Temporal.configure do |config|
    config.host = 'localhost'
    config.port = 7233
    config.namespace = 'ruby-samples'
    config.task_queue = 'hello-world'
    config.credentials = GRPC::Core::ChannelCredentials.new(root_cert, client_key, client_chain)
end
```

### OAuth2 Token

Use gRPC Call Credentials to add OAuth2 token to gRPC calls:

```ruby
Temporal.configure do |config|
    config.host = 'localhost'
    config.port = 7233
    config.namespace = 'ruby-samples'
    config.task_queue = 'hello-world'
    config.credentials = GRPC::Core::CallCredentials.new(updater_proc)
end
```
`updater_proc` should be a method that returns `proc`. See an example of `updater_proc` in [googleauth](https://www.rubydoc.info/gems/googleauth/0.1.0/Signet/OAuth2/Client) library.

### Combining Credentials

To configure both SSL and OAuth2 token cedentials use `compose` method:

```ruby
Temporal.configure do |config|
    config.host = 'localhost'
    config.port = 7233
    config.namespace = 'ruby-samples'
    config.task_queue = 'hello-world'
    config.credentials = GRPC::Core::ChannelCredentials.new(root_cert, client_key, client_chain).compose(
        GRPC::Core::CallCredentials.new(token.updater_proc)
    )
end
```

## Configuration

This gem is optimised for the smoothest out-of-the-box experience, which is achieved using a global
configuration:

```ruby
Temporal.configure do |config|
  config.host = '127.0.0.1' # sets global host
  ...
end

Temporal::Worker.new # uses global host
Temporal.start_workflow(...) # uses global host
```

This will work just fine for simpler use-cases, however at some point you might need to setup
multiple clients and workers within the same instance of your app (e.g. you have different Temporal
hosts, need to use different codecs/converters for different parts of your app, etc). Should this be
the case we recommend using explicit local configurations for each client/worker:

```ruby
config_1 = Temporal::Configuration.new
config_1.host = 'temporal-01'

config_2 = Temporal::Configuration.new
config_2.host = 'temporal-01'

worker_1 = Temporal::Worker.new(config_1)
worker_2 = Temporal::Worker.new(config_2)

client_1 = Temporal::Client.new(config_1)
client_1.start_workflow(...)

client_2 = Temporal::Client.new(config_2)
client_2.start_workflow(...)
```

*NOTE: Almost all the methods on the `Temporal` module are delegated to the default client that's
initialized using global configuration. The same methods can be used directly on your own client
instances.*

## Workflows

A workflow is defined using pure Ruby code, however it should contain only a high-level
deterministic outline of the steps (their composition) that need to be executed to complete a
workflow. The actual work should be defined in your activities.

*NOTE: Keep in mind that your workflow code can get run multiple times (replayed) during the same
execution, which is why it must NOT contain any non-deterministic code (network requests, DB
queries, etc) as it can break your workflows.*

Here's an example workflow:

```ruby
class RenewSubscriptionWorkflow < Temporal::Workflow
  def execute(user_id)
    subscription = FetchUserSubscriptionActivity.execute!(user_id)
    subscription ||= CreateUserSubscriptionActivity.execute!(user_id)

    return if subscription[:active]

    ChargeCreditCardActivity.execute!(subscription[:price], subscription[:card_token])

    RenewedSubscriptionActivity.execute!(subscription[:id])
    SendSubscriptionRenewalEmailActivity.execute!(user_id, subscription[:id])
  rescue CreditCardNotChargedError => e
    CancelSubscriptionActivity.execute!(subscription[:id])
    SendSubscriptionCancellationEmailActivity.execute!(user_id, subscription[:id])
  end
end
```

In this simple workflow we are checking if a user has an active subscription and then attempt to
charge their credit card to renew an expired subscription, notifying the user of the outcome. All
the work is encapsulated in activities, while the workflow itself is responsible for calling the
activities in the right order, passing values between them and handling failures.

There is a couple of ways to execute an activity from your workflow:

```ruby
# Calls the activity by its class and blocks the execution until activity is
# finished. The return value of your activity will get assigned to the result
result = MyActivity.execute!(arg1, arg2)

# Here's a non-blocking version of the execute, returning back the future that
# will get fulfilled when activity completes. This approach allows modelling
# asynchronous workflows with activities executed in parallel
future = MyActivity.execute(arg1, arg2)
result = future.get

# Full versions of the calls from above, but has more flexibility (shown below)
result = workflow.execute_activity!(MyActivity, arg1, arg2)
future = workflow.execute_activity(MyActivity, arg1, arg2)

# In case your workflow code does not have access to activity classes (separate
# process, activities implemented in a different language, etc), you can
# simply reference them by their names
workflow.execute_activity('MyActivity', arg1, arg2, options: { namespace: 'my-namespace', task_queue: 'my-task-queue' })
```

Besides calling activities workflows can:

- Use timers
- Receive signals
- Execute other (child) workflows
- Respond to queries


## Activities

An activity is a basic unit of work that performs the desired action (potentially causing
side-effects). It can return a result or raise an error. It is defined like so:

```ruby
class CloseUserAccountActivity < Temporal::Activity
  class UserNotFound < Temporal::ActivityException; end

  def execute(user_id)
    user = User.find_by(id: user_id)

    raise UserNotFound, 'User with specified ID does not exist' unless user

    user.close_account
    user.save

    AccountClosureEmail.deliver(user)

    return nil
  end
end
```

It is important to make your activities **idempotent**, because they can get retried by Temporal (in
case a timeout is reached or your activity has thrown an error). You normally want to avoid
generating additional side effects during subsequent activity execution.

To achieve this there are two methods (returning a UUID token) available from your activity class:

- `activity.run_idem` — unique within for the current workflow execution (scoped to run_id)
- `activity.workflow_idem` — unique across all execution of the workflow (scoped to workflow_id)

Both tokens will remain the same across multiple retry attempts of the activity.

### Asynchronous completion

When dealing with asynchronous business logic in your activities, you might need to wait for an
external event to complete your activity (e.g. a callback or a webhook). This can be achieved by
manually completing your activity using a provided `async_token` from activity's context:

```ruby
class AsyncActivity < Temporal::Activity
  def execute(user_id)
    user = User.find_by(id: user_id)

    # Pass the async_token to complete your activity later
    ExternalSystem.verify_user(user, activity.async_token)

    activity.async # prevents activity from completing immediately
  end
end
```

Later when a confirmation is received you'll need to complete your activity manually using the token
provided:

```ruby
Temporal.complete_activity(async_token, result)
```

Similarly you can fail the activity by calling:

```ruby
Temporal.fail_activity(async_token, MyError.new('Something went wrong'))
```

This doesn't change the behaviour from the workflow's perspective — as any other activity the result
will be returned or an error raised.

*NOTE: Make sure to configure your timeouts accordingly and not to set heartbeat timeout (off by
default) since you won't be able to emit heartbeats and your async activities will keep timing out.*

Similar behaviour can also be achieved in other ways (one which might be more preferable in your
specific use-case), e.g.:

- by polling for a result within your activity (long-running activities with heartbeat)
- using retry policy to keep retrying activity until a result is available
- completing your activity after the initial call is made, but then waiting on a completion signal
from your workflow


## Worker

Worker is a process that communicates with the Temporal server and manages Workflow and Activity
execution. To start a worker:

```ruby
require 'temporal/worker'

worker = Temporal::Worker.new
worker.register_workflow(HelloWorldWorkflow)
worker.register_activity(SomeActivity)
worker.register_activity(SomeOtherActivity)
worker.start
```

A call to `worker.start` will take over the current process and will keep it unning until a `TERM`
or `INT` signal is received. By only registering a subset of your workflows/activities with a given
worker you can split processing across as many workers as you need.


## Starting a workflow

All communication is handled via Temporal service, so in order to start a workflow you need to send
a message to Temporal:

```ruby
Temporal.start_workflow(HelloWorldWorkflow)
```

Optionally you can pass input and other options to the workflow:

```ruby
Temporal.start_workflow(RenewSubscriptionWorkflow, user_id, options: { workflow_id: user_id })
```

Passing in a `workflow_id` allows you to prevent concurrent execution of a workflow — a subsequent
call with the same `workflow_id` will always get rejected while it is still running, raising
`Temporal::WorkflowExecutionAlreadyStartedFailure`. You can adjust the behaviour for finished
workflows by supplying the `workflow_id_reuse_policy:` argument with one of these options:

- `:allow_failed` will allow re-running workflows that have failed (terminated, cancelled, timed out or failed)
- `:allow` will allow re-running any finished workflows both failed and completed
- `:reject` will reject any subsequent attempt to run a workflow

For an **already running** workflow, `Temporal.start_workflow` can instead return its existing run ID:

```ruby
run_id = Temporal.start_workflow(RenewSubscriptionWorkflow, user_id,
  options: { workflow_id: user_id, workflow_id_conflict_policy: :use_existing })
```

This is opt-in. The existing workflow keeps its original input; arguments in the second start call
are not delivered to it. The default remains to raise `Temporal::WorkflowExecutionAlreadyStartedFailure`.
The separate `workflow_id_reuse_policy` controls what happens after the prior run closes.
`workflow_id_conflict_policy` is supported for ordinary `start_workflow` calls and the
Update-With-Start APIs below, not signal-with-start, `schedule_workflow`, or
`Temporal::Testing.local!`.



## Workflow Updates and Update-With-Start

Updates are requests with a durable result. A handler can validate a request, change workflow state,
await Activities or timers, and return a value or an error to its caller. Unlike a query, an accepted
Update is recorded in history and is replayed after a worker restart. Unlike a signal, it has a result.
This implementation is tested against **Temporal Server 1.32.0**. Deploy workers with Update support
before enabling clients that send Updates to their task queues.

Register handlers synchronously in `execute`, before its first blocking operation. Update arguments
are positional, just like workflow and Activity arguments. A validator raises to reject a request;
its return value is ignored. Validators must be deterministic, synchronous and read-only: no state
mutation, I/O, Activities, timers, Future callbacks, side effects or blocking. Validators are **not** run again when
replaying an already accepted Update.

```ruby
class BillingCoordinator < Temporal::Workflow
  def execute(account_id)
    busy = false
    stopped = false

    workflow.on_update('cancel_invoice', validator: ->(invoice_id) {
      raise ArgumentError, 'invoice ID required' if invoice_id.to_s.empty?
    }) do |invoice_id|
      # Handlers interleave whenever they await an Activity/timer/condition.
      # Serialize operations that must observe and modify consistent shared state.
      workflow.wait_until { !busy }
      busy = true
      begin
        # The Activity must make its external operation idempotent. A stable
        # business operation ID can be passed to the external system as its key.
        CancelInvoiceActivity.execute!(account_id, invoice_id, workflow.current_update[:id])
      ensure
        busy = false
      end
    end

    workflow.on_signal('stop') { stopped = true }
    workflow.wait_until { stopped }
    workflow.wait_until { workflow.all_update_handlers_finished? }
  end
end

result = Temporal.execute_update_with_start_workflow(
  BillingCoordinator, 'cancel_invoice', 'invoice-123',
  start_args: ['account-456'],
  options: { workflow_id: 'billing-account-456', task_queue: 'billing',
             workflow_id_reuse_policy: :allow },
  workflow_id_conflict_policy: :use_existing,
  update_id: 'cancel-request-789',
  request_id: 'start-request-789',
  timeout: 60
)
```

This uses the server's atomic `ExecuteMultiOperation` (Start + Update) API. If no run exists, the
server starts one and delivers the Update. For a running workflow, the required conflict policy is
`:use_existing` or `:fail`. The existing `workflow_id_reuse_policy` controls closed workflows
(`:allow`, `:allow_failed`, or `:reject`). Start arguments/options are only applied when starting a
new run. Cron schedules, start delays and Signal-With-Start options cannot be combined with this API.
A validator rejection does **not** undo a workflow that was started by Update-With-Start.

To return after durable acceptance and wait for the result later:

```ruby
handle = Temporal.start_update_with_start_workflow(
  BillingCoordinator, 'cancel_invoice', 'invoice-123',
  start_args: ['account-456'], options: { workflow_id: 'billing-account-456' },
  workflow_id_conflict_policy: :use_existing, update_id: 'cancel-request-789',
  wait_for_stage: :accepted # :completed is also supported
)
result = handle.result(timeout: 60)

# Save all four fields before exiting the client process:
# handle.namespace, handle.workflow_id, handle.run_id, handle.update_id
saved = Temporal.get_workflow_update_handle(
  handle.workflow_id, handle.run_id, handle.update_id, namespace: handle.namespace
)
result = saved.result(timeout: 60)

# Update an already running execution, with no start operation:
result = Temporal.execute_workflow_update(
  BillingCoordinator, 'cancel_invoice', handle.workflow_id, handle.run_id, 'invoice-124',
  update_id: 'cancel-request-790', timeout: 60
)
# start_workflow_update has the same arguments and returns an accepted handle.
```

`WorkflowUpdateFailed` represents a validator rejection or handler exception; `cause` is the
converted application error. Handler errors fail the Update, leaving the Workflow running. RPC
failures remain gRPC errors; expiration of the overall wait budget raises `Temporal::TimeoutError`.
A timeout does not cancel the Update. Reuse both IDs after an ambiguous Update-With-Start failure,
or retrieve the handle by the saved run ID. The SDK retains request IDs across its internal retries.
`workflow_started` on a handle reflects the first successful server start response; after a lost
response it cannot establish which caller originally created the run.

Update IDs deduplicate **within one Workflow run**, not across new runs or reset branches. They do
not provide exactly-once external effects: handlers replay, Activities retry, and resets can reapply
Updates. Use idempotent Activities and stable business keys at the external boundary. Validators and
handlers must follow the same determinism/versioning rules as the rest of the Workflow. Update
headers use the configured converter and header propagators; a handler can read decoded headers
from `workflow.current_update[:headers]` and its ID/name from `:id`/`:name`.

Normal workflow completion waits for accepted handlers to finish. Before continuing as new,
explicitly wait for `workflow.all_update_handlers_finished?`; a handler cannot complete, fail, or
continue the Workflow itself. Future `done`/`failed` callbacks registered inside an Update inherit
that Update's context and restrictions, even if another part of the workflow created the Future.
The Update waits for these callbacks as well as the handler to finish. A callback exception is
delivered to the suspended handler's rescue/ensure blocks, or fails the Update if the handler has
already returned. Other owned callbacks still finish before the outcome is sent. Do not make a
callback wait for its own Update to complete. External termination/timeouts can still interrupt
in-flight Updates.
The local `Temporal::Testing.local!` simulator does not implement Updates and raises rather than
sending these requests to a server. Use the integration suite with an isolated Temporal test server:

```sh
temporal server start-dev --headless --ip 127.0.0.1 --port 17233
TEMPORAL_UPDATE_INTEGRATION=1 TEMPORAL_UPDATE_PORT=17233 \
  bundle exec rspec spec/integration/update_with_start_spec.rb
```

The generated protobuf additions are a narrow backport of the Update/ExecuteMultiOperation surface
from [Temporal API v1.44.0](https://github.com/temporalio/api/tree/v1.44.0/temporal/api).
`make proto` applies `scripts/backport_workflow_updates.rb` after ordinary generation; the script can
also be run directly and is idempotent. Other API definitions and the proto submodule pin are retained.

## Execution Options

There are lots of ways in which you can configure your Workflows and Activities. The common ones
(namespace, task_queue, timeouts and retry policy) can be defined in one of these places (in the order
of precedence):

1. Inline when starting or registering a workflow/activity (use `options:` argument)
2. In your workflow/activity class definitions by calling a class method (e.g. `namespace 'my-namespace'`)
3. Globally, when configuring your Temporal library via `Temporal.configure`


## Periodic workflow execution

In certain cases you might need a workflow that runs periodically using a cron schedule. This can be
achieved using the `Temporal.schedule_workflow` API that take a periodic cron schedule as a second
argument:

```ruby
Temporal.schedule_workflow(HealthCheckWorkflow, '*/5 * * * *')
```

This will instruct Temporal to run a HealthCheckWorkflow every 5 minutes. All the rest of the
arguments are identical to the `Temporal.start_workflow` API.

*NOTE: Your execution timeout will be measured across all the workflow invocations, so make sure to
set it to allow as many invocations as you need. You can also set it to `nil`, which will use a
default value of 10 years.*

## Middleware
Middleware sits between the execution of your workflows/activities and the Temporal SDK, allowing you to insert custom code before or after the execution.

### Activity Middleware Stack
Middleware added to the activity middleware stack will be executed around each activity method. This is useful when you want to perform a certain task before and/or after each activity execution, such as logging, error handling, or measuring execution time.

### Workflow Middleware Stack
There are actually two types of workflow middleware in Temporal Ruby SDK:

*Workflow Middleware*: This middleware is executed around each entire workflow. This is similar to activity middleware, but for workflows.

*Workflow Task Middleware*: This middleware is executed around each workflow task, of which there will be many for each workflow.

### Example
To add a middleware, you need to define a class that responds to the call method. Within the call method, you should call yield to allow the next middleware in the stack (or the workflow/activity method itself if there are no more middlewares) to execute. Here's an example:

```
class MyMiddleware
  def call(metadata)
    puts "Before execution"
    yield
    puts "After execution"
    result
  end
end
```

You can add this middleware to the stack like so `worker.add_activity_middleware(MyMiddleware)`

Please note that the order of middleware in the stack matters. The middleware that is added last will be the first one to execute. In the example above, MyMiddleware will execute before any other middleware in the stack.

## Breaking Changes

Since the workflow execution has to be deterministic, breaking changes can not be simply added and
deployed — this will undermine the consistency of running workflows and might lead to unexpected
behaviour. However, breaking changes are often needed and these include:

- Adding new activities, timers, child workflows, etc.
- Remove existing activities, timers, child workflows, etc.
- Rearranging existing activities, timers, child workflows, etc.
- Adding/removing signal handlers

In order to add a breaking change you can use `workflow.has_release?(release_name)` method in your
workflows, which is guaranteed to return a consistent result whether or not it was called prior to
shipping the new release. It is also consistent for all the subsequent calls with the same
`release_name` — all of them will return the original result. Consider the following example:

```ruby
class MyWorkflow < Temporal::Workflow
  def execute
    ActivityOld1.execute!

    workflow.sleep(10)

    ActivityOld2.execute!

    return nil
  end
end
```

which got updated to:

```ruby
class MyWorkflow < Temporal::Workflow
  def execute
    Activity1.execute!

    if workflow.has_release?(:fix_1)
      ActivityNew1.execute!
    end

    workflow.sleep(10)

    if workflow.has_release?(:fix_1)
      ActivityNew2.execute!
    else
      ActivityOld.execute!
    end

    if workflow.has_release?(:fix_2)
      ActivityNew3.execute!
    end

    return nil
  end
end
```

If the release got deployed while the original workflow was waiting on a timer, `ActivityNew1` and
`ActivityNew2` won't get executed, because they are part of the same change (same release_name),
however `ActivityNew3` will get executed, since the release wasn't yet checked at the time. And for
every new execution of the workflow — all new activities will get executed, while `ActivityOld` will
not.

Later on you can clean it up and drop all the checks if you don't have any older workflows running
or expect them to ever be executed (e.g. reset).

*NOTE: Releases with different names do not depend on each other in any way.*

## Testing

It is crucial to properly test your workflows and activities before running them in production. The
provided testing framework is still limited in functionality, but will allow you to test basic
use-cases.

The testing framework is not required automatically when you require `temporal-ruby`, so you have to
do this yourself (it is strongly recommended to only include this in your test environment,
`spec_helper.rb` or similar):

```ruby
require 'temporal/testing'
```

This will allow you to execute workflows locally by running `HelloWorldWorkflow.execute_locally`.
Any arguments provided will forwarded to your `#execute` method.

In case of a higher level end-to-end integration specs, where you need to execute a Temporal workflow
as part of your code, you can enable local testing:

```ruby
Temporal::Testing.local!
```

This will treat every `Temporal.start_workflow` call as local and perform your workflows inline. It
also works with a block, restoring the original mode back after the execution:

```ruby
Temporal::Testing.local! do
  Temporal.start_workflow(HelloWorldWorkflow)
end
```

Make sure to check out [example integration specs](examples/spec/integration) for more details. Instructions
for running these integration specs can be found in [examples/README.md](examples/README.md).


## TODO

There's plenty of work to be done, but most importanly we need:

- Write specs for everything
- Implement support for missing features


## LICENSE

Copyright 2020 Coinbase, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

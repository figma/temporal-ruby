require 'temporal/workflow/context'
require 'temporal/workflow/query_registry'

describe Temporal::Workflow::UpdateExecution do
  let(:config) { Temporal::Configuration.new }
  let(:dispatcher) { Temporal::Workflow::Dispatcher.new }
  let(:state) { Temporal::Workflow::StateManager.new(dispatcher, config) }
  let(:context) do
    Temporal::Workflow::Context.new(state, dispatcher, Temporal::Workflow, Fabricate(:workflow_metadata),
      config, Temporal::Workflow::QueryRegistry.new, false)
  end
  let(:info) { { id: 'owning-update', name: 'callback-test', headers: { 'trace' => 'trace-id' } }.freeze }
  let(:completions) { [] }
  let(:execution) { described_class.new(context, info) { |result, error| completions << [result, error] } }
  # Deliberately create outside the Update; registration determines ownership.
  let!(:future) { Temporal::Workflow::Future.new(:shared_future, context) }

  before { context.on_update('registered') {} }

  def resolve(callback_type)
    callback_type == :done ? future.set('activity-result') : future.fail(StandardError.new('activity-failure'))
    callbacks = callback_type == :done ? future.success_callbacks : future.failure_callbacks
    callbacks.each { |callback| context.send(:call_in_fiber, callback, 'callback-argument') }
  end

  # Each closure guard is distinct; callback ownership is shared by both paths.
  [[:done, :complete], [:failed, :fail], [:done, :continue_as_new]].each do |callback_type, close_method|
    it "preserves Update ownership in #{callback_type}, guards #{close_method}, and unwinds its suspended handler" do
      seen = nil
      unwound = false
      execution.start do
        future.public_send(callback_type) do
          seen = context.current_update
          argument = close_method == :fail ? StandardError.new('must not fail workflow') : nil
          context.public_send(close_method, argument)
        end
        begin
          context.wait_until { false }
        ensure
          unwound = true
        end
      end
      expect(completions).to be_empty
      resolve(callback_type)
      expect(seen).to eq(info)
      expect(unwound).to eq(true)
      expect(completions.size).to eq(1)
      expect(completions.first.last).to be_a(ArgumentError)
      expect(state.final_commands).to be_empty
      expect(context.current_update).to be_nil
      expect(Thread.current[described_class::CURRENT_KEY]).to be_nil
    end
  end

  it 'delivers callback errors to the handler rescue block instead of stranding a wait condition' do
    execution.start do
      future.done { context.complete }
      begin
        context.wait_until { false }
      rescue ArgumentError
        'handled callback failure'
      end
    end
    resolve(:done)
    expect(completions).to eq([['handled callback failure', nil]])
  end

  it 'waits for an owned callback after the main handler has returned, including across another yield' do
    released = false
    seen = nil
    execution.start do
      future.done do
        context.wait_until { released }
        seen = context.current_update
      end
      'handler-result'
    end
    expect(completions).to be_empty
    resolve(:done)
    expect(completions).to be_empty
    released = true
    dispatcher.dispatch(:release, 'released')
    expect(completions).to eq([['handler-result', nil]])
    expect(seen).to eq(info)
  end

  it 'fails the still-pending Update if its callback raises ScriptError after the handler returns' do
    execution.start do
      future.done { raise NotImplementedError, 'callback not implemented' }
      'must not report success early'
    end
    expect(completions).to be_empty
    resolve(:done)
    expect(completions.size).to eq(1)
    expect(completions.first.last).to be_a(NotImplementedError)
  end

  it 'discards the unselected success/failure callback so it cannot hold the Update open forever' do
    invoked = []
    execution.start do
      future.done { invoked << :done }
      future.failed { invoked << :failed }
      'result'
    end
    resolve(:done)
    expect(invoked).to eq([:done])
    expect(completions).to eq([['result', nil]])
  end

  it 'preserves closure guards and failure delivery for an already-fulfilled Future callback' do
    future.set('already-ready')
    execution.start { future.done { context.complete } }
    expect(completions.first.last).to be_a(ArgumentError)
    expect(state.final_commands).to be_empty
  end

  it 'keeps callbacks from concurrent Updates and the main workflow separate on a shared Future' do
    seen = []
    other_completions = []
    other_info = { id: 'other-update', name: 'callback-test', headers: {} }.freeze
    other = described_class.new(context, other_info) { |result, error| other_completions << [result, error] }
    future.done { seen << context.current_update }
    execution.start do
      future.done do
        seen << context.current_update
        raise ArgumentError, 'first update failed'
      end
      'first result'
    end
    other.start do
      future.done { seen << context.current_update }
      'other result'
    end
    resolve(:done)
    expect(seen).to eq([nil, info, other_info])
    expect(completions.first.last).to be_a(ArgumentError)
    expect(other_completions).to eq([['other result', nil]])
  end
end

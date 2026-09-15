# Manual credit / flow control: setting `flow_control: false` disables the built-in
# MessageCount strategy entirely, leaving it up to `handle_message/2` to call `credit/1`
# whenever it's ready for more. Here we simulate slow processing and only ask for the next
# chunk once we're done, instead of letting the server flood us with all of them upfront.
#
# Note: `get_credits/0`, like `store_offset/0`, is a `GenServer.call` back to this same
# consumer process, so it can't be called from within `handle_message/1-3`, which already
# run inside it (it would deadlock). `credit/1` is safe to call from in here because it's a
# cast. To read the current credit count from inside a callback, use `handle_message/2` and
# read `state.credits` directly, as below.
#
# Run with:
#   mix run examples/manual_credit/manual_credit.exs

stream_name = "manual-credit-example-stream"

defmodule ManualCredit.Connection do
  use RabbitMQStream.Connection
end

defmodule ManualCredit.Producer do
  use RabbitMQStream.Producer,
    connection: ManualCredit.Connection,
    stream_name: stream_name
end

defmodule ManualCredit.Consumer do
  use RabbitMQStream.Consumer,
    connection: ManualCredit.Connection,
    stream_name: stream_name,
    initial_offset: :first,
    initial_credit: 1,
    flow_control: false

  @impl true
  def handle_message(message, state) do
    IO.puts("Received: #{message} (credits left: #{state.credits})")

    # Simulate slow processing, then ask for exactly one more chunk.
    Process.sleep(200)
    credit(1)

    :ok
  end
end

{:ok, _connection} = ManualCredit.Connection.start_link()

case ManualCredit.Connection.create_stream(stream_name) do
  :ok -> :ok
  {:error, :stream_already_exists} -> :ok
end

{:ok, _producer} = ManualCredit.Producer.start_link()

for i <- 1..5 do
  ManualCredit.Producer.publish("message-#{i}")
  # Small delay so the broker is more likely to send these as separate chunks.
  Process.sleep(50)
end

{:ok, _consumer} = ManualCredit.Consumer.start_link()

Process.sleep(2_000)

ManualCredit.Connection.delete_stream(stream_name)

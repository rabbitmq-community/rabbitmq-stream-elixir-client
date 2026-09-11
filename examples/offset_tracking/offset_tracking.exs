# Manually storing the consumer's offset, instead of relying on one of the built-in
# strategies. Passing `offset_tracking: []` disables every automatic strategy, so it is up
# to us to store it, via `handle_chunk/2`.
#
# Note: `store_offset/0` (the wrapper the `use` macro injects) issues a `GenServer.call`
# back to this same consumer process, so it can only be called from *outside* the consumer.
# Calling it from within `handle_message/1-3` or `handle_chunk/1-2`, which already run inside
# the consumer process, deadlocks it. That's why we call `RabbitMQStream.Connection.store_offset/4`
# directly below, the same way the built-in strategies do internally.
#
# Run with:
#   mix run examples/offset_tracking/offset_tracking.exs

stream_name = "offset-tracking-example-stream"

defmodule OffsetTracking.Connection do
  use RabbitMQStream.Connection
end

defmodule OffsetTracking.Producer do
  use RabbitMQStream.Producer,
    connection: OffsetTracking.Connection,
    stream_name: stream_name
end

defmodule OffsetTracking.Consumer do
  use RabbitMQStream.Consumer,
    connection: OffsetTracking.Connection,
    stream_name: stream_name,
    initial_offset: :first,
    offset_tracking: []

  @impl true
  def handle_message(message) do
    IO.puts("Received: #{message}")
    :ok
  end

  @impl true
  def handle_chunk(chunk, state) do
    RabbitMQStream.Connection.store_offset(
      state.connection,
      state.stream_name,
      state.offset_reference,
      chunk.chunk_id + chunk.num_entries
    )
  end
end

{:ok, _connection} = OffsetTracking.Connection.start_link()

case OffsetTracking.Connection.create_stream(stream_name) do
  :ok -> :ok
  {:error, :stream_already_exists} -> :ok
end

{:ok, _producer} = OffsetTracking.Producer.start_link()

for i <- 0..4 do
  OffsetTracking.Producer.publish("message-#{i}")
end

Process.sleep(200)

{:ok, _consumer} = OffsetTracking.Consumer.start_link()

Process.sleep(500)

{:ok, offset} =
  OffsetTracking.Connection.query_offset(stream_name, Atom.to_string(OffsetTracking.Consumer))

IO.puts("Offset stored on the stream for this consumer: #{offset}")

OffsetTracking.Connection.delete_stream(stream_name)

# Automatically storing the consumer's offset, using the built-in `count` strategy: the offset
# is persisted to the stream every time `store_after` messages are processed.
#
# RabbitMQ tracks progress by `offset_reference`, not by consumer process. So a brand new
# consumer that reuses the same `offset_reference` as a previous one resumes exactly where that
# previous one left off, without ever seeing the messages it already processed. That's what this
# example demonstrates: `FirstConsumer` processes an initial batch and stops, then a second batch
# is published, and `ResumedConsumer` (sharing the same `offset_reference`) only receives that
# second batch.
#
# Run with:
#   mix run examples/automatic_offset_tracking/automatic_offset_tracking.exs

stream_name = "automatic-offset-tracking-example-stream"
offset_reference = "automatic-offset-tracking-example-consumer"

defmodule AutomaticOffsetTracking.Connection do
  use RabbitMQStream.Connection
end

defmodule AutomaticOffsetTracking.Producer do
  use RabbitMQStream.Producer,
    connection: AutomaticOffsetTracking.Connection,
    stream_name: stream_name
end

defmodule AutomaticOffsetTracking.FirstConsumer do
  use RabbitMQStream.Consumer,
    connection: AutomaticOffsetTracking.Connection,
    stream_name: stream_name,
    offset_reference: offset_reference,
    initial_offset: :first,
    offset_tracking: [count: [store_after: 5]]

  @impl true
  def handle_message(message) do
    IO.puts("[first-consumer]   #{message}")
    :ok
  end
end

defmodule AutomaticOffsetTracking.ResumedConsumer do
  use RabbitMQStream.Consumer,
    connection: AutomaticOffsetTracking.Connection,
    stream_name: stream_name,
    offset_reference: offset_reference,
    initial_offset: :first,
    offset_tracking: [count: [store_after: 5]]

  @impl true
  def handle_message(message) do
    IO.puts("[resumed-consumer] #{message}")
    :ok
  end
end

{:ok, _connection} = AutomaticOffsetTracking.Connection.start_link()

case AutomaticOffsetTracking.Connection.create_stream(stream_name) do
  :ok -> :ok
  {:error, :stream_already_exists} -> :ok
end

{:ok, _producer} = AutomaticOffsetTracking.Producer.start_link()

IO.puts("--- publishing first batch of 10 messages ---")
for i <- 1..10, do: AutomaticOffsetTracking.Producer.publish("message-#{i}")
Process.sleep(200)

{:ok, _first_consumer} = AutomaticOffsetTracking.FirstConsumer.start_link()

# Give the count strategy time to fire after every 5th message.
Process.sleep(1_000)

AutomaticOffsetTracking.FirstConsumer.stop()

{:ok, offset} = AutomaticOffsetTracking.Connection.query_offset(stream_name, offset_reference)
IO.puts("--- offset automatically stored so far: #{offset} ---")

IO.puts("--- publishing second batch of 5 messages ---")
for i <- 11..15, do: AutomaticOffsetTracking.Producer.publish("message-#{i}")
Process.sleep(200)

IO.puts(
  "--- starting a brand new consumer that reuses the same offset_reference: it should only receive the second batch ---"
)

{:ok, _resumed_consumer} = AutomaticOffsetTracking.ResumedConsumer.start_link()

Process.sleep(1_000)

AutomaticOffsetTracking.ResumedConsumer.stop()
AutomaticOffsetTracking.Connection.delete_stream(stream_name)

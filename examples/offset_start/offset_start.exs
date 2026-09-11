# How to set different points to start consuming from: :first, :next, and {:offset, n}.
#
# Run with:
#   mix run examples/offset_start/offset_start.exs

stream_name = "offset-start-example-stream"

defmodule OffsetStart.Connection do
  use RabbitMQStream.Connection
end

defmodule OffsetStart.Producer do
  use RabbitMQStream.Producer,
    connection: OffsetStart.Connection,
    stream_name: stream_name
end

defmodule OffsetStart.FirstConsumer do
  use RabbitMQStream.Consumer,
    connection: OffsetStart.Connection,
    stream_name: stream_name,
    initial_offset: :first

  @impl true
  def handle_message(message) do
    IO.puts("[:first]          #{message}")
    :ok
  end
end

defmodule OffsetStart.OffsetConsumer do
  use RabbitMQStream.Consumer,
    connection: OffsetStart.Connection,
    stream_name: stream_name,
    initial_offset: {:offset, 2}

  @impl true
  def handle_message(message) do
    IO.puts("[{:offset, 2}]    #{message}")
    :ok
  end
end

defmodule OffsetStart.NextConsumer do
  use RabbitMQStream.Consumer,
    connection: OffsetStart.Connection,
    stream_name: stream_name,
    initial_offset: :next

  @impl true
  def handle_message(message) do
    IO.puts("[:next]           #{message}")
    :ok
  end
end

{:ok, _connection} = OffsetStart.Connection.start_link()

case OffsetStart.Connection.create_stream(stream_name) do
  :ok -> :ok
  {:error, :stream_already_exists} -> :ok
end

{:ok, _producer} = OffsetStart.Producer.start_link()

for i <- 0..4 do
  OffsetStart.Producer.publish("existing-message-#{i}")
end

Process.sleep(200)

IO.puts("--- starting consumers: :first and {:offset, 2} will replay the 5 messages above, :next won't ---")

{:ok, _} = OffsetStart.FirstConsumer.start_link()
{:ok, _} = OffsetStart.OffsetConsumer.start_link()
{:ok, _} = OffsetStart.NextConsumer.start_link()

Process.sleep(500)

IO.puts("--- publishing 3 new messages: all three consumers will receive these ---")

for i <- 0..2 do
  OffsetStart.Producer.publish("new-message-#{i}")
end

Process.sleep(500)

OffsetStart.Connection.delete_stream(stream_name)

# Producer and Consumer, the structures you need to start.
#
# Run with:
#   mix run examples/getting_started/getting_started.exs

stream_name = "getting-started-stream"

defmodule GettingStarted.Connection do
  use RabbitMQStream.Connection
end

defmodule GettingStarted.Consumer do
  use RabbitMQStream.Consumer,
    connection: GettingStarted.Connection,
    stream_name: stream_name,
    initial_offset: :first

  @impl true
  def handle_message(message) do
    IO.puts("Received: #{message}")
    :ok
  end
end

defmodule GettingStarted.Producer do
  use RabbitMQStream.Producer,
    connection: GettingStarted.Connection,
    stream_name: stream_name
end

{:ok, _connection} = GettingStarted.Connection.start_link()

case GettingStarted.Connection.create_stream(stream_name) do
  :ok -> :ok
  {:error, :stream_already_exists} -> :ok
end

{:ok, _producer} = GettingStarted.Producer.start_link()
{:ok, _consumer} = GettingStarted.Consumer.start_link()

for i <- 1..10 do
  GettingStarted.Producer.publish("Hello, world #{i}!")
end

Process.sleep(1_000)

GettingStarted.Connection.delete_stream(stream_name)

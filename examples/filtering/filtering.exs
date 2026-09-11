# Stream filtering (RabbitMQ 3.13+): the producer tags each message with a `filter_value`,
# and each consumer only asks the server for chunks that might contain messages matching
# the filter values it declares. Messages are formatted as "<region>|<payload>" purely so
# this example can derive a filter_value from them without a serializer.
#
# Run with:
#   mix run examples/filtering/filtering.exs

stream_name = "filtering-example-stream"

defmodule Filtering.Connection do
  use RabbitMQStream.Connection
end

defmodule Filtering.Producer do
  use RabbitMQStream.Producer,
    connection: Filtering.Connection,
    stream_name: stream_name

  @impl true
  def filter_value(message) do
    message |> String.split("|", parts: 2) |> List.first()
  end
end

defmodule Filtering.EuConsumer do
  use RabbitMQStream.Consumer,
    connection: Filtering.Connection,
    stream_name: stream_name,
    initial_offset: :first,
    properties: [filter: ["eu"]]

  @impl true
  def filter_value(message) do
    message |> String.split("|", parts: 2) |> List.first()
  end

  @impl true
  def handle_message(message) do
    IO.puts("[eu-consumer]    #{message}")
    :ok
  end
end

defmodule Filtering.LatamConsumer do
  use RabbitMQStream.Consumer,
    connection: Filtering.Connection,
    stream_name: stream_name,
    initial_offset: :first,
    properties: [filter: ["latam"]]

  @impl true
  def filter_value(message) do
    message |> String.split("|", parts: 2) |> List.first()
  end

  @impl true
  def handle_message(message) do
    IO.puts("[latam-consumer] #{message}")
    :ok
  end
end

{:ok, _connection} = Filtering.Connection.start_link()

case Filtering.Connection.create_stream(stream_name) do
  :ok -> :ok
  {:error, :stream_already_exists} -> :ok
end

{:ok, _producer} = Filtering.Producer.start_link()
{:ok, _eu_consumer} = Filtering.EuConsumer.start_link()
{:ok, _latam_consumer} = Filtering.LatamConsumer.start_link()

for i <- 1..3 do
  Filtering.Producer.publish("eu|order-#{i}")
  Filtering.Producer.publish("latam|order-#{i}")
end

Process.sleep(1_000)

Filtering.Connection.delete_stream(stream_name)

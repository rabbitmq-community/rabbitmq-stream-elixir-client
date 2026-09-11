# Super Stream example with Single Active Consumer across two instances. See README.md in
# this folder for the scenario.
#
# Run with:
#   mix run examples/super_stream/super_stream.exs

super_stream = "super-stream-example"
offset_reference = "super-stream-example-consumer"

defmodule SuperStream.ProducerConnection do
  use RabbitMQStream.Connection
end

defmodule SuperStream.Producer do
  use RabbitMQStream.SuperProducer,
    connection: SuperStream.ProducerConnection,
    super_stream: super_stream,
    partitions: 2

  @impl true
  def routing_key(message, _partitions) do
    message |> String.split("|", parts: 2) |> List.first()
  end
end

defmodule SuperStream.ConnectionA do
  use RabbitMQStream.Connection
end

defmodule SuperStream.ConsumerA do
  use RabbitMQStream.SuperConsumer,
    connection: SuperStream.ConnectionA,
    super_stream: super_stream,
    offset_reference: offset_reference,
    initial_offset: :first,
    offset_tracking: [count: [store_after: 2]],
    partitions: 2

  @impl true
  def handle_update(consumer, :upgrade) do
    case RabbitMQStream.Connection.query_offset(consumer.connection, consumer.stream_name, consumer.offset_reference) do
      {:ok, offset} -> {:ok, {:offset, offset}}
      _ -> {:ok, :first}
    end
  end

  @impl true
  def handle_update(_consumer, :downgrade), do: {:ok, :next}

  @impl true
  def handle_message(message) do
    IO.puts("[instance-a] #{message}")
    :ok
  end
end

defmodule SuperStream.ConnectionB do
  use RabbitMQStream.Connection
end

defmodule SuperStream.ConsumerB do
  use RabbitMQStream.SuperConsumer,
    connection: SuperStream.ConnectionB,
    super_stream: super_stream,
    offset_reference: offset_reference,
    initial_offset: :first,
    offset_tracking: [count: [store_after: 2]],
    partitions: 2

  @impl true
  def handle_update(consumer, :upgrade) do
    case RabbitMQStream.Connection.query_offset(consumer.connection, consumer.stream_name, consumer.offset_reference) do
      {:ok, offset} -> {:ok, {:offset, offset}}
      _ -> {:ok, :first}
    end
  end

  @impl true
  def handle_update(_consumer, :downgrade), do: {:ok, :next}

  @impl true
  def handle_message(message) do
    IO.puts("[instance-b] #{message}")
    :ok
  end
end

{:ok, _producer_connection} = SuperStream.ProducerConnection.start_link()

case SuperStream.ProducerConnection.create_super_stream(super_stream,
       eu: ["#{super_stream}-eu"],
       latam: ["#{super_stream}-latam"]
     ) do
  :ok -> :ok
  {:error, :stream_already_exists} -> :ok
end

{:ok, _producer} = SuperStream.Producer.start_link([])

IO.puts("--- starting instance A: it becomes the active consumer on every partition ---")
{:ok, _connection_a} = SuperStream.ConnectionA.start_link()
{:ok, _} = SuperStream.ConsumerA.start_link([])
Process.sleep(1_000)

IO.puts("--- starting instance B: the server immediately rebalances one partition to it ---")
{:ok, _connection_b} = SuperStream.ConnectionB.start_link()
{:ok, _} = SuperStream.ConsumerB.start_link([])
Process.sleep(1_000)

IO.puts("--- publishing while both instances are active, each on its own partition ---")

for i <- 1..3 do
  SuperStream.Producer.publish("eu|order-#{i}")
  SuperStream.Producer.publish("latam|order-#{i}")
end

Process.sleep(500)

IO.puts("--- stopping instance A entirely (consumer + connection): instance B takes over the rest ---")
Supervisor.stop(SuperStream.ConsumerA)
SuperStream.ConnectionA.stop()
Process.sleep(1_000)

IO.puts("--- publishing again: instance B now receives everything ---")

for i <- 4..6 do
  SuperStream.Producer.publish("eu|order-#{i}")
  SuperStream.Producer.publish("latam|order-#{i}")
end

Process.sleep(500)

Supervisor.stop(SuperStream.ConsumerB)
SuperStream.ConnectionB.stop()
SuperStream.ProducerConnection.delete_super_stream(super_stream)

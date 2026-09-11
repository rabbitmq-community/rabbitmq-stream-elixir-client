# Single Active Consumer example. See README.md in this folder for the scenario.
#
# Run with:
#   mix run examples/single_active_consumer/single_active_consumer.exs

stream_name = "sac-example-stream"
group_name = "sac-example-group"

defmodule SAC.Connection do
  use RabbitMQStream.Connection
end

defmodule SAC.Producer do
  use RabbitMQStream.Producer,
    connection: SAC.Connection,
    stream_name: stream_name
end

defmodule SAC.ConsumerA do
  use RabbitMQStream.Consumer,
    connection: SAC.Connection,
    stream_name: stream_name,
    initial_offset: :first,
    properties: [single_active_consumer: group_name]

  @impl true
  def handle_update(_consumer, :upgrade) do
    IO.puts("[consumer-a] upgraded to active")
    {:ok, :next}
  end

  @impl true
  def handle_update(_consumer, :downgrade) do
    IO.puts("[consumer-a] downgraded to inactive")
    {:ok, :next}
  end

  @impl true
  def handle_message(message) do
    IO.puts("[consumer-a] #{message}")
    :ok
  end
end

defmodule SAC.ConsumerB do
  use RabbitMQStream.Consumer,
    connection: SAC.Connection,
    stream_name: stream_name,
    initial_offset: :first,
    properties: [single_active_consumer: group_name]

  @impl true
  def handle_update(_consumer, :upgrade) do
    IO.puts("[consumer-b] upgraded to active")
    {:ok, :next}
  end

  @impl true
  def handle_update(_consumer, :downgrade) do
    IO.puts("[consumer-b] downgraded to inactive")
    {:ok, :next}
  end

  @impl true
  def handle_message(message) do
    IO.puts("[consumer-b] #{message}")
    :ok
  end
end

{:ok, _connection} = SAC.Connection.start_link()

case SAC.Connection.create_stream(stream_name) do
  :ok -> :ok
  {:error, :stream_already_exists} -> :ok
end

{:ok, _producer} = SAC.Producer.start_link()

IO.puts("--- starting consumer-a: it becomes the active consumer of the group ---")
{:ok, _} = SAC.ConsumerA.start_link()
Process.sleep(300)

for i <- 1..3, do: SAC.Producer.publish("first-batch-#{i}")
Process.sleep(500)

IO.puts("--- starting consumer-b: it joins the group but stays inactive ---")
{:ok, _} = SAC.ConsumerB.start_link()
Process.sleep(300)

for i <- 4..6, do: SAC.Producer.publish("first-batch-#{i}")
Process.sleep(500)

IO.puts("--- stopping consumer-a: consumer-b is promoted to active ---")
SAC.ConsumerA.stop()
Process.sleep(500)

for i <- 1..3, do: SAC.Producer.publish("second-batch-#{i}")
Process.sleep(500)

SAC.ConsumerB.stop()
SAC.Connection.delete_stream(stream_name)

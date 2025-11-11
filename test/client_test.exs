defmodule RabbitMQStreamTest.ClientTest do
  use ExUnit.Case, async: false

  # @tag :v3_13_proxied_cluster
  # test "should auto discover and connect to all node when behind a loadbalancer" do
  # end

  defmodule TheClient do
    use RabbitMQStream.Client, host: "localhost"
  end

  defmodule ClientProducer do
    use RabbitMQStream.Producer
  end

  # @tag :v3_13_cluster
  test "should create a stream" do
    {:ok, conn} = RabbitMQStream.Connection.start_link(host: "localhost")

    assert :ok = RabbitMQStream.Connection.connect(conn)

    {:ok, client} = RabbitMQStream.Client.start_link(host: "localhost")

    RabbitMQStream.Connection.create_stream(conn, "stream1")

    {:ok, _subscription_id} = RabbitMQStream.Connection.subscribe(client, "stream1", self(), :next, 999)

    {:ok, _} =
      ClientProducer.start_link(
        connection: client,
        reference_name: "client-producer",
        stream_name: "stream1"
      )

    message = Jason.encode!(%{message: "Hello, world2!"})

    ClientProducer.publish(message)

    assert_receive {:deliver, %{osiris_chunk: %{data_entries: [^message]}}}, 500
  end
end

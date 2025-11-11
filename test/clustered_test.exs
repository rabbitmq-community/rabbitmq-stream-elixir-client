defmodule RabbitMQStreamTest.Clustered do
  use ExUnit.Case, async: false

  # @tag :v3_13_proxied_cluster
  # test "should auto discover and connect to all node when behind a loadbalancer" do
  # end

  defmodule TheClient do
    use RabbitMQStream.Client, host: "rabbitmq1"
  end

  # @tag :v3_13_cluster
  test "should connect to all the replicated nodes" do
    {:ok, conn1} = RabbitMQStream.Connection.start_link(host: "rabbitmq1")
    {:ok, conn2} = RabbitMQStream.Connection.start_link(host: "rabbitmq2")
    {:ok, conn3} = RabbitMQStream.Connection.start_link(host: "rabbitmq3")

    assert :ok = RabbitMQStream.Connection.connect(conn1)
    assert :ok = RabbitMQStream.Connection.connect(conn2)
    assert :ok = RabbitMQStream.Connection.connect(conn3)

    RabbitMQStream.Connection.create_stream(conn1, "stream1")
    RabbitMQStream.Connection.create_stream(conn2, "stream2")
    RabbitMQStream.Connection.create_stream(conn3, "stream3")

    dbg(RabbitMQStream.Connection.query_metadata(conn1, ["stream1", "stream2", "stream3"]))
  end

  @tag :v3_13_cluster
  test "should auto discover and connect to all nodes" do
    {:ok, conn1} = RabbitMQStream.Connection.start_link(host: "rabbitmq1")
    {:ok, conn2} = RabbitMQStream.Connection.start_link(host: "rabbitmq2")
    {:ok, conn3} = RabbitMQStream.Connection.start_link(host: "rabbitmq3")

    {:ok, client} = RabbitMQStream.Client.start_link(host: "rabbitmq1")

    RabbitMQStream.Connection.create_stream(conn1, "stream1")
    RabbitMQStream.Connection.create_stream(conn2, "stream2")
    RabbitMQStream.Connection.create_stream(conn3, "stream3")

    assert {:ok, _subscription_id} = RabbitMQStream.Connection.subscribe(client, "stream1", self(), :next, 999)
    assert {:ok, _subscription_id} = RabbitMQStream.Connection.subscribe(client, "stream2", self(), :next, 999)
    assert {:ok, _subscription_id} = RabbitMQStream.Connection.subscribe(client, "stream3", self(), :next, 999)

    assert {:ok, _subscription_id} = RabbitMQStream.Connection.subscribe(client, "stream1", self(), :next, 999)
    assert {:ok, _subscription_id} = RabbitMQStream.Connection.subscribe(client, "stream2", self(), :next, 999)
    assert {:ok, _subscription_id} = RabbitMQStream.Connection.subscribe(client, "stream3", self(), :next, 999)
  end
end

defmodule RabbitMQStreamTest.Clustered do
  use ExUnit.Case, async: false

  @tag :v3_13_proxied_cluster
  test "should auto discover and connect to all node when behind a loadbalancer" do
  end

  @tag :v3_13_cluster
  test "should auto discover and connect to all nodes" do
    {:ok, conn1} = RabbitMQStream.Connection.start_link(host: "rabbitmq1")
    {:ok, conn2} = RabbitMQStream.Connection.start_link(host: "rabbitmq2")
    {:ok, conn3} = RabbitMQStream.Connection.start_link(host: "rabbitmq3")

    assert :ok = RabbitMQStream.Connection.connect(conn1)
    assert :ok = RabbitMQStream.Connection.connect(conn2)
    assert :ok = RabbitMQStream.Connection.connect(conn3)

    assert :ok = RabbitMQStream.Connection.create_stream(conn1, "stream1")
    assert :ok = RabbitMQStream.Connection.create_stream(conn2, "stream2")
    assert :ok = RabbitMQStream.Connection.create_stream(conn3, "stream3")

    {:ok, %{streams: streams, brokers: brokers}} =
      RabbitMQStream.Connection.query_metadata(conn1, ["stream1", "stream2", "stream3"])

    assert Enum.all?(streams, &(&1.code == :ok))

    # conn1 only ever talks to rabbitmq1 directly; seeing all 3 broker
    # entries proves cluster-wide topology was actually discovered, not
    # just the local node's view.
    assert brokers |> Enum.map(& &1.host) |> Enum.sort() == ["rabbitmq1", "rabbitmq2", "rabbitmq3"]

    brokers_by_ref = Map.new(brokers, &{&1.reference, &1.host})
    leader_host = fn stream_name -> brokers_by_ref[Enum.find(streams, &(&1.name == stream_name)).leader] end

    assert leader_host.("stream1") == "rabbitmq1"
    assert leader_host.("stream2") == "rabbitmq2"
    assert leader_host.("stream3") == "rabbitmq3"

    :ok = RabbitMQStream.Connection.delete_stream(conn1, "stream1")
    :ok = RabbitMQStream.Connection.delete_stream(conn2, "stream2")
    :ok = RabbitMQStream.Connection.delete_stream(conn3, "stream3")
  end
end

defmodule RabbitMQStreamTest.Clustered do
  use ExUnit.Case, async: false

  alias RabbitMQStream.Connection.MetadataResolver

  defmodule TestConsumer do
    use RabbitMQStream.Consumer

    @impl true
    def handle_message(entry, %{private: parent}) do
      send(parent, {:message, entry})
      :ok
    end
  end

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

  @tag :v3_13_cluster
  test "producer and consumer route directly to the stream's leader/replica, not the seed connection" do
    {:ok, conn1} = RabbitMQStream.Connection.start_link(host: "rabbitmq1")
    {:ok, conn2} = RabbitMQStream.Connection.start_link(host: "rabbitmq2")
    {:ok, conn3} = RabbitMQStream.Connection.start_link(host: "rabbitmq3")

    assert :ok = RabbitMQStream.Connection.connect(conn1)
    assert :ok = RabbitMQStream.Connection.connect(conn2)
    assert :ok = RabbitMQStream.Connection.connect(conn3)

    conns = [conn1, conn2, conn3]
    streams = for n <- 1..6, do: "leader-routing-test-#{n}"

    # Create the streams from a round-robin of the 3 nodes. RabbitMQ's leader-locator
    # strategy decides actual placement, not necessarily the creating node — several
    # streams are used so at least one is very likely to resolve to a non-seed leader,
    # avoiding a flaky test tied to a specific placement.
    for {stream, conn} <- Enum.zip(streams, Stream.cycle(conns)) do
      RabbitMQStream.Connection.delete_stream(conn1, stream)
      assert :ok = RabbitMQStream.Connection.create_stream(conn, stream)
    end

    {:ok, metadata} = RabbitMQStream.Connection.query_metadata(conn1, streams)

    resolved =
      for stream <- streams, into: %{} do
        {:ok, resolution} = MetadataResolver.resolve(metadata, stream)
        {stream, resolution}
      end

    # Ground truth for "which node is the leader" is the client's own resolved
    # `query_metadata` output, cross-referenced against each seed connection's live
    # options — not `rabbitmqctl`/the management API, which isn't exposed on this
    # cluster's docker-compose today. This validates that the client's routing
    # decision matches its own leader-resolution decision, end-to-end.
    node_of = fn host, port ->
      Enum.find(conns, fn conn ->
        opts = RabbitMQStream.Connection.get_options(conn)
        opts[:host] == host and opts[:port] == port
      end)
    end

    {routed_stream, %{leader: leader} = routed_resolution} =
      Enum.find(resolved, fn {_stream, %{leader: leader}} -> node_of.(leader.host, leader.port) != conn1 end) ||
        raise "none of the test streams' leaders landed away from conn1 — cluster topology assumption broken"

    {:ok, producer} =
      RabbitMQStream.Producer.start_link(
        connection: conn1,
        stream_name: routed_stream,
        reference_name: "leader-routing-producer"
      )

    producer_state = :sys.get_state(producer)
    producer_conn_opts = RabbitMQStream.Connection.get_options(producer_state.connection)

    assert producer_conn_opts[:host] == leader.host
    assert producer_conn_opts[:port] == leader.port
    assert producer_state.connection != producer_state.seed_connection

    {:ok, _consumer} =
      TestConsumer.start_link(
        connection: conn1,
        stream_name: routed_stream,
        initial_offset: :next,
        private: self()
      )

    consumer_state = :sys.get_state(Process.whereis(TestConsumer))
    consumer_conn_opts = RabbitMQStream.Connection.get_options(consumer_state.connection)

    candidates = [leader | routed_resolution.replicas]
    assert Enum.any?(candidates, &(&1.host == consumer_conn_opts[:host] and &1.port == consumer_conn_opts[:port]))

    message = "leader-routing-message"
    :ok = RabbitMQStream.Producer.publish(producer, message)

    assert_receive {:message, ^message}, 1000

    for stream <- streams, do: RabbitMQStream.Connection.delete_stream(conn1, stream)
  end
end

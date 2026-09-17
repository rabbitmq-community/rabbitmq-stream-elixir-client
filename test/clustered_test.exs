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

  defmodule TestSuperConsumer do
    use RabbitMQStream.SuperConsumer, initial_offset: :next, partitions: 3

    @impl true
    def handle_message(entry, %{private: parent}) do
      send(parent, {:message, entry})
      :ok
    end

    @impl true
    def handle_update(state, _action) do
      {:ok, state.initial_offset}
    end
  end

  defmodule TestSuperProducer do
    use RabbitMQStream.SuperProducer, partitions: 3
  end

  # SuperConsumer.start_link/1 (a Supervisor) returns as soon as its Manager's
  # init/1 does, before the Manager's handle_continue/2 has started (let alone
  # subscribed) every partition consumer. Force synchronization on the Manager
  # first so the Registry is fully populated, then on each partition consumer
  # so its subscribe (sent from its own handle_continue/2) is acknowledged by
  # the broker, avoiding a race with `publish` where a not-yet-subscribed
  # consumer silently misses messages.
  defp wait_super_consumer_ready(module) do
    :sys.get_state(Module.concat(module, Manager))

    module
    |> Module.concat(Registry)
    |> Registry.select([{{:_, :"$1", :_}, [], [:"$1"]}])
    |> Enum.each(&RabbitMQStream.Consumer.get_credits/1)
  end

  # A SuperProducer/SuperConsumer's Registry keys are the partition's stream
  # name itself (see `SuperProducer.Manager`/`SuperConsumer.Manager`), so this
  # doubles as a stream-name -> pid map for correlating against
  # `MetadataResolver.resolve/2` output.
  defp super_stream_partition_children(module) do
    module
    |> Module.concat(Registry)
    |> Registry.select([{{:"$1", :"$2", :_}, [], [{{:"$1", :"$2"}}]}])
  end

  # Even after the subscribe request above is acknowledged, the broker appears
  # to need a brief moment to actually activate delivery for a freshly created
  # subscription -- observed empirically as an intermittent missed message
  # immediately after wait_super_consumer_ready/1 returns. This margin is a
  # pragmatic buffer for that broker-side activation latency, not a substitute
  # for the synchronization above.
  @broker_subscription_activation_margin 500

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

  @tag :v3_13_cluster
  test "super stream partitions each route their producer/consumer to their own leader/replica" do
    {:ok, conn1} = RabbitMQStream.Connection.start_link(host: "rabbitmq1")
    assert :ok = RabbitMQStream.Connection.connect(conn1)

    super_stream = "super-leader-routing-test"
    partitions = for n <- 0..2, do: "#{super_stream}-#{n}"

    RabbitMQStream.Connection.delete_super_stream(conn1, super_stream)

    # create_super_stream/4 is a single RPC with no lever to force partitions onto
    # specific nodes (unlike plain create_stream/2, which the test above round-robins
    # across seed connections) -- actual placement is entirely up to the cluster's
    # queue-leader-locator policy. Explicit single-stream-per-routing-key partitions
    # are used so the resulting stream names are predictable without depending on the
    # PARTITIONS command's naming behavior.
    assert :ok =
             RabbitMQStream.Connection.create_super_stream(conn1, super_stream,
               "0": [Enum.at(partitions, 0)],
               "1": [Enum.at(partitions, 1)],
               "2": [Enum.at(partitions, 2)]
             )

    {:ok, metadata} = RabbitMQStream.Connection.query_metadata(conn1, partitions)

    resolved =
      for partition <- partitions, into: %{} do
        {:ok, resolution} = MetadataResolver.resolve(metadata, partition)
        {partition, resolution}
      end

    leader_hosts = resolved |> Map.values() |> Enum.map(& &1.leader.host) |> Enum.uniq()

    unless length(leader_hosts) > 1 do
      raise "all #{length(partitions)} super stream partitions' leaders landed on a single node — cluster topology assumption broken"
    end

    {:ok, _} = TestSuperProducer.start_link(connection: conn1, super_stream: super_stream)

    {:ok, _} =
      TestSuperConsumer.start_link(connection: conn1, super_stream: super_stream, private: self())

    wait_super_consumer_ready(TestSuperConsumer)

    producer_states =
      for {partition, pid} <- super_stream_partition_children(TestSuperProducer), into: %{} do
        {partition, :sys.get_state(pid)}
      end

    consumer_states =
      for {partition, pid} <- super_stream_partition_children(TestSuperConsumer), into: %{} do
        {partition, :sys.get_state(pid)}
      end

    assert Enum.sort(Map.keys(producer_states)) == Enum.sort(partitions)
    assert Enum.sort(Map.keys(consumer_states)) == Enum.sort(partitions)

    # Every partition's producer must target its own leader exactly, same ground truth
    # (the client's own query_metadata/MetadataResolver.resolve output) as the
    # plain-stream leader-routing test above.
    for {partition, state} <- producer_states do
      assert state.seed_connection == conn1

      opts = RabbitMQStream.Connection.get_options(state.connection)
      leader = resolved[partition].leader

      assert opts[:host] == leader.host
      assert opts[:port] == leader.port
    end

    assert Enum.any?(producer_states, fn {_, state} -> state.connection != state.seed_connection end),
           "expected at least one super stream partition's producer to route away from the seed connection"

    # Consumers may land on any replica, not necessarily the leader (see
    # Router.consumer_connection/2's random-replica selection).
    for {partition, state} <- consumer_states do
      assert state.seed_connection == conn1

      opts = RabbitMQStream.Connection.get_options(state.connection)
      candidates = [resolved[partition].leader | resolved[partition].replicas]

      assert Enum.any?(candidates, &(&1.host == opts[:host] and &1.port == opts[:port]))
    end

    Process.sleep(@broker_subscription_activation_margin)

    message = "super-stream-leader-routing-message"
    :ok = TestSuperProducer.publish(message)

    assert_receive {:message, ^message}, 1000

    RabbitMQStream.Connection.delete_super_stream(conn1, super_stream)
  end
end

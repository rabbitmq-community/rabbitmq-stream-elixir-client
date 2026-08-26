defmodule RabbitMQStreamTest.SuperStream do
  use ExUnit.Case, async: false
  alias RabbitMQStream.OsirisChunk

  defmodule SuperConsumer1 do
    use RabbitMQStream.SuperConsumer,
      initial_offset: :next,
      partitions: 3

    @impl true
    def handle_chunk(%OsirisChunk{}, %{private: parent}) do
      send(parent, __MODULE__)

      :ok
    end

    @impl true
    def handle_update(state, _) do
      {:ok, state.initial_offset}
    end
  end

  defmodule SuperConsumer2 do
    use RabbitMQStream.SuperConsumer,
      initial_offset: :next,
      partitions: 3

    @impl true
    def handle_chunk(%OsirisChunk{}, %{private: parent}) do
      send(parent, __MODULE__)

      :ok
    end

    @impl true
    def handle_update(state, _) do
      {:ok, state.initial_offset}
    end
  end

  defmodule SuperConsumer3 do
    use RabbitMQStream.SuperConsumer,
      initial_offset: :next,
      partitions: 3

    @impl true
    def handle_chunk(%OsirisChunk{}, %{private: parent}) do
      send(parent, __MODULE__)

      :ok
    end

    @impl true
    def handle_update(state, _) do
      {:ok, state.initial_offset}
    end
  end

  defmodule SuperProducer1 do
    use RabbitMQStream.SuperProducer,
      partitions: 3
  end

  defmodule SuperProducer2 do
    use RabbitMQStream.SuperProducer

    @impl true
    def routing_key(message, _) do
      case message do
        "1" ->
          "route-A"

        _ ->
          "route-B"
      end
    end
  end

  setup do
    {:ok, conn} = RabbitMQStream.Connection.start_link(host: "localhost", vhost: "/")
    :ok = RabbitMQStream.Connection.connect(conn)

    [conn: conn]
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

  # Even after the subscribe request above is acknowledged, the broker
  # appears to need a brief moment to actually activate delivery for a batch
  # of subscriptions created in quick succession (9 here: 3 consumers x 3
  # partitions each) -- observed empirically as an intermittent missed
  # message immediately after wait_super_consumer_ready/1 returns. This
  # margin is a pragmatic buffer for that broker-side activation latency,
  # not a substitute for the synchronization above.
  @broker_subscription_activation_margin 300

  @tag :v3_13
  @tag :v4_2
  @tag :v4_3
  test "should create and delete a super_stream", %{conn: conn} do
    RabbitMQStream.Connection.delete_super_stream(conn, "transactions")

    :ok =
      RabbitMQStream.Connection.create_super_stream(conn, "transactions",
        "route-A": ["stream-01", "stream-02"],
        "route-B": ["stream-03"]
      )

    {:ok, %{streams: streams}} = RabbitMQStream.Connection.route(conn, "route-A", "transactions")

    assert Enum.all?(streams, fn stream -> stream in ["stream-01", "stream-02"] end)

    {:ok, %{streams: streams}} = RabbitMQStream.Connection.route(conn, "route-B", "transactions")
    assert Enum.all?(streams, fn stream -> stream in ["stream-03"] end)

    {:ok, %{streams: []}} = RabbitMQStream.Connection.route(conn, "route-C", "transactions")

    {:ok, %{streams: streams}} = RabbitMQStream.Connection.partitions(conn, "transactions")

    assert Enum.all?(streams, fn stream -> stream in ["stream-01", "stream-02", "stream-03"] end)

    for consumer <- [SuperConsumer1, SuperConsumer2, SuperConsumer3] do
      {:ok, conn} = RabbitMQStream.Connection.start_link(host: "localhost", vhost: "/")
      :ok = RabbitMQStream.Connection.connect(conn)

      {:ok, _} =
        consumer.start_link(
          connection: conn,
          super_stream: "transactions",
          private: self()
        )

      wait_super_consumer_ready(consumer)
    end

    {:ok, _} =
      SuperProducer2.start_link(
        connection: conn,
        super_stream: "transactions"
      )

    Process.sleep(@broker_subscription_activation_margin)

    :ok = SuperProducer2.publish("1")
    :ok = SuperProducer2.publish("12")
    :ok = SuperProducer2.publish("123")

    # Each consumer subscribes to every partition, so a single publish can be
    # observed by all three; drain until each has reported rather than
    # assuming the first 3 arrivals are one-per-consumer.
    modules =
      Enum.reduce_while(1..9, MapSet.new(), fn _, acc ->
        receive do
          msg ->
            acc = MapSet.put(acc, msg)
            if MapSet.size(acc) == 3, do: {:halt, acc}, else: {:cont, acc}
        after
          500 -> {:halt, acc}
        end
      end)

    assert SuperConsumer1 in modules
    assert SuperConsumer2 in modules
    assert SuperConsumer3 in modules

    :ok = RabbitMQStream.Connection.delete_super_stream(conn, "transactions")
  end

  @tag :v3_11
  @tag :v3_12
  @tag :v3_13
  @tag :v4_2
  @tag :v4_3
  test "should create super streams", %{conn: conn} do
    {:ok, %{streams: streams}} =
      RabbitMQStream.Connection.query_metadata(conn, ["invoices-0", "invoices-1", "invoices-2"])

    unless Enum.all?(streams, &(&1.code == :ok)) do
      raise "SuperStream streams were not found. Please ensure you've created the \"invoices\" SuperStream with 3 partitions using RabbitMQ CLI or Management UI before running this test."
    end

    for consumer <- [SuperConsumer1, SuperConsumer2, SuperConsumer3] do
      {:ok, conn} = RabbitMQStream.Connection.start_link(host: "localhost", vhost: "/")
      :ok = RabbitMQStream.Connection.connect(conn)

      {:ok, _} =
        consumer.start_link(
          connection: conn,
          super_stream: "invoices",
          private: self()
        )

      wait_super_consumer_ready(consumer)
    end

    {:ok, _} =
      SuperProducer1.start_link(
        connection: conn,
        super_stream: "invoices"
      )

    Process.sleep(@broker_subscription_activation_margin)

    :ok = SuperProducer1.publish("1")
    :ok = SuperProducer1.publish("12")
    :ok = SuperProducer1.publish("123")

    # Each consumer subscribes to every partition, so a single publish can be
    # observed by all three; drain until each has reported rather than
    # assuming the first 3 arrivals are one-per-consumer.
    modules =
      Enum.reduce_while(1..9, MapSet.new(), fn _, acc ->
        receive do
          msg ->
            acc = MapSet.put(acc, msg)
            if MapSet.size(acc) == 3, do: {:halt, acc}, else: {:cont, acc}
        after
          500 -> {:halt, acc}
        end
      end)

    assert SuperConsumer1 in modules
    assert SuperConsumer2 in modules
    assert SuperConsumer3 in modules
  end
end

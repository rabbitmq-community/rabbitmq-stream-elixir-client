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

  @tag :v3_13
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
    end

    {:ok, _} =
      SuperProducer2.start_link(
        connection: conn,
        super_stream: "transactions"
      )

    # We wait a bit to guarantee that the consumers are ready
    Process.sleep(500)

    :ok = SuperProducer2.publish("1")
    :ok = SuperProducer2.publish("12")
    :ok = SuperProducer2.publish("123")

    msgs =
      for _ <- 1..3 do
        receive do
          msg -> msg
        after
          500 -> :timeout
        end
      end

    # Process.sleep(60_000)
    assert SuperConsumer1 in msgs
    assert SuperConsumer2 in msgs
    assert SuperConsumer3 in msgs

    :ok = RabbitMQStream.Connection.delete_super_stream(conn, "transactions")
  end

  @tag :v3_11
  @tag :v3_12
  @tag :v3_13
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
    end

    {:ok, _} =
      SuperProducer1.start_link(
        connection: conn,
        super_stream: "invoices"
      )

    # We wait a bit to guarantee that the consumers are ready
    Process.sleep(500)

    :ok = SuperProducer1.publish("1")
    :ok = SuperProducer1.publish("12")
    :ok = SuperProducer1.publish("123")

    msgs =
      for _ <- 1..3 do
        receive do
          msg -> msg
        after
          500 -> :timeout
        end
      end

    # Process.sleep(60_000)
    assert SuperConsumer1 in msgs
    assert SuperConsumer2 in msgs
    assert SuperConsumer3 in msgs
  end
end

defmodule RabbitMQStreamTest.Consumer do
  use ExUnit.Case, async: false
  alias RabbitMQStream.OsirisChunk

  @moduletag :v3_11
  @moduletag :v3_12
  @moduletag :v3_13
  @moduletag :v4_2
  @moduletag :v4_3

  defmodule SupervisedConnection do
    use RabbitMQStream.Connection
  end

  defmodule SupervisedConnection2 do
    use RabbitMQStream.Connection
  end

  defmodule SupervisorProducer do
    use RabbitMQStream.Producer,
      connection: SupervisedConnection

    @impl true
    def before_start(_opts, state) do
      RabbitMQStream.Connection.create_stream(state.connection, state.stream_name)

      state
    end
  end

  defmodule SupervisorProducer2 do
    use RabbitMQStream.Producer,
      connection: SupervisedConnection,
      serializer: Jason

    @impl true
    def before_start(_opts, state) do
      RabbitMQStream.Connection.create_stream(state.connection, state.stream_name)

      state
    end
  end

  defmodule Consumer do
    use RabbitMQStream.Consumer,
      connection: SupervisedConnection,
      serializer: Jason

    @impl true
    def handle_message(entry, %{private: parent}) do
      send(parent, {:message, entry})

      :ok
    end
  end

  # SupervisedConnection/SupervisedConnection2 are registered under a fixed
  # name, so the next test's start_link races the previous test process's
  # implicit linked-process teardown unless we stop it synchronously first.
  # on_exit runs in a separate process, so the pid found by whereis/1 can
  # still die from that same link teardown before GenServer.stop/1 reaches
  # it; treat that as already-stopped rather than letting it crash on_exit.
  setup do
    on_exit(fn ->
      for mod <- [SupervisedConnection, SupervisedConnection2] do
        case Process.whereis(mod) do
          nil ->
            :ok

          pid ->
            try do
              GenServer.stop(pid)
            catch
              :exit, _ -> :ok
            end
        end
      end
    end)

    {:ok, _conn} = SupervisedConnection.start_link(host: "localhost", vhost: "/")
    :ok = SupervisedConnection.connect()

    {:ok, _conn} = SupervisedConnection2.start_link(host: "localhost", vhost: "/")
    :ok = SupervisedConnection2.connect()

    :ok
  end

  @stream "consumer-test-stream-01"
  @reference_name "reference-01"
  test "should publish and receive a message" do
    {:ok, _producer} = SupervisorProducer.start_link(reference_name: @reference_name, stream_name: @stream)

    assert {:ok, subscription_id} = SupervisedConnection.subscribe(@stream, self(), :next, 999)

    message = Jason.encode!(%{message: "Hello, world2!"})

    SupervisorProducer.publish(message)

    assert_receive {:deliver, %{osiris_chunk: %OsirisChunk{data_entries: [^message]}}}, 500

    assert :ok = SupervisedConnection.unsubscribe(subscription_id)

    SupervisorProducer.publish(message)

    refute_receive {:deliver, %{osiris_chunk: %OsirisChunk{}}}, 500
    SupervisedConnection.delete_stream(@stream)
  end

  @stream "consumer-test-stream-02"
  @reference_name "reference-02"
  test "should credit a consumer" do
    {:ok, _producer} = SupervisorProducer.start_link(reference_name: @reference_name, stream_name: @stream)

    # We ensure the stream exists before consuming
    SupervisedConnection.create_stream(@stream)
    assert {:ok, subscription_id} = SupervisedConnection.subscribe(@stream, self(), :next, 1)

    message = Jason.encode!(%{message: "Hello, world!"})

    SupervisorProducer.publish(message)

    assert_receive {:deliver, %{osiris_chunk: %OsirisChunk{data_entries: [^message]}}}, 500

    message = Jason.encode!(%{message: "Hello, world2!"})

    SupervisorProducer.publish(message)

    refute_receive {:deliver, %{osiris_chunk: %OsirisChunk{}}}, 500

    assert :ok = SupervisedConnection.credit(subscription_id, 1)

    assert_receive {:deliver, %{osiris_chunk: %OsirisChunk{data_entries: [^message]}}}, 500
    SupervisedConnection.delete_stream(@stream)
  end

  @stream "consumer-test-stream-10"
  @reference_name "reference-10"
  test "a message should be received by a persistent consumer" do
    SupervisedConnection.delete_stream(@stream)

    {:ok, _producer} =
      SupervisorProducer2.start_link(reference_name: @reference_name, stream_name: @stream)

    {:ok, _subscriber} =
      Consumer.start_link(
        initial_offset: :next,
        stream_name: @stream,
        private: self(),
        offset_tracking: [count: [store_after: 1]]
      )

    # start_link/1 returns as soon as init/1 does, before the subscribe request
    # (sent from handle_continue/2) is acknowledged by the broker. Since GenServer
    # guarantees handle_continue completes before any queued call is answered, this
    # call blocks until the subscription is active, avoiding a race with `publish`
    # below when `initial_offset: :next` is used.
    Consumer.get_credits()

    message1 = %{"message" => "Consumer Test: 1"}
    message2 = %{"message" => "Consumer Test: 2"}
    message3 = %{"message" => "Consumer Test: 3"}
    message4 = %{"message" => "Consumer Test: 4"}
    message5 = %{"message" => "Consumer Test: 5"}
    message6 = %{"message" => "Consumer Test: 6"}
    message7 = %{"message" => "Consumer Test: 7"}
    message8 = %{"message" => "Consumer Test: 8"}
    message9 = %{"message" => "Consumer Test: 9"}
    message10 = %{"message" => "Consumer Test: 10"}

    SupervisorProducer2.publish(message1)
    assert_receive {:message, ^message1}, 500

    SupervisorProducer2.publish(message2)
    assert_receive {:message, ^message2}, 500

    :ok = GenServer.stop(Consumer, :normal)

    SupervisorProducer2.publish(message3)
    SupervisorProducer2.publish(message4)
    SupervisorProducer2.publish(message5)
    SupervisorProducer2.publish(message6)
    SupervisorProducer2.publish(message7)
    SupervisorProducer2.publish(message8)
    SupervisorProducer2.publish(message9)
    SupervisorProducer2.publish(message10)

    {:ok, _subscriber} =
      Consumer.start_link(
        initial_offset: :next,
        stream_name: @stream,
        private: self(),
        offset_tracking: [count: [store_after: 1]]
      )

    refute_receive {:message, ^message1}, 500
    refute_receive {:message, ^message2}, 500
    assert_receive {:message, ^message3}, 500
    assert_receive {:message, ^message4}, 500
    assert_receive {:message, ^message5}, 500
    assert_receive {:message, ^message6}, 500
    assert_receive {:message, ^message7}, 500
    assert_receive {:message, ^message8}, 500
    assert_receive {:message, ^message9}, 500
    assert_receive {:message, ^message10}, 500

    SupervisedConnection.delete_stream(@stream)
  end

  @stream "consumer-test-stream-04"
  @reference_name "reference-04"
  test "should not duplicate messages when the reference_name is used twice" do
    SupervisedConnection.create_stream(@stream)

    {:ok, _subscriber} =
      Consumer.start_link(
        initial_offset: :next,
        stream_name: @stream,
        private: self(),
        offset_tracking: [count: [store_after: 1]]
      )

    {:ok, first} =
      RabbitMQStream.Producer.start_link(
        connection: SupervisedConnection,
        stream_name: @stream,
        reference_name: "#{@reference_name}"
      )

    {:ok, second} =
      RabbitMQStream.Producer.start_link(
        connection: SupervisedConnection2,
        stream_name: @stream,
        reference_name: "#{@reference_name}"
      )

    Process.sleep(1000)

    :ok = RabbitMQStream.Producer.publish(first, Jason.encode!(%{message: "first"}))
    Process.sleep(500)
    :ok = RabbitMQStream.Producer.publish(second, Jason.encode!(%{message: "second"}))

    assert_receive {:message, %{"message" => "first"}}, 500
    refute_receive {:message, %{"message" => "second"}}, 500
  end
end

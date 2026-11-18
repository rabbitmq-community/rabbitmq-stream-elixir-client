defmodule RabbitMQStreamTest.Producer do
  use ExUnit.Case, async: false
  import ExUnit.CaptureLog
  alias RabbitMQStream.Connection

  @moduletag :v3_11
  @moduletag :v3_12
  @moduletag :v3_13
  @moduletag :v4_2
  @moduletag :v4_3

  defmodule SupervisedConnection do
    use RabbitMQStream.Connection
  end

  defmodule SupervisorProducer do
    use RabbitMQStream.Producer,
      connection: RabbitMQStreamTest.Producer.SupervisedConnection

    @impl true
    def before_start(_opts, state) do
      RabbitMQStream.Connection.create_stream(state.connection, state.stream_name)

      state
    end
  end

  defmodule ConfirmingProducer do
    use RabbitMQStream.Producer,
      connection: RabbitMQStreamTest.Producer.SupervisedConnection

    @impl true
    def before_start(_opts, state) do
      RabbitMQStream.Connection.create_stream(state.connection, state.stream_name)

      state
    end

    @impl true
    def handle_confirm(publishing_ids) do
      send(Process.whereis(:producer_confirm_test_listener), {:confirmed, publishing_ids})
    end

    @impl true
    def handle_publish_error(errors) do
      send(Process.whereis(:producer_confirm_test_listener), {:publish_error, errors})
    end
  end

  # Generous timeout for slow CI runners; a broker rejection fails right away with its reason.
  defp assert_confirmed do
    receive do
      {:confirmed, [_publishing_id]} -> :ok
      {:publish_error, errors} -> flunk("Publish rejected by the broker: #{inspect(errors)}")
    after
      5_000 -> flunk("No publish confirm within 5s")
    end
  end

  defmodule CustomRoutingProducer do
    use RabbitMQStream.Producer,
      connection: RabbitMQStreamTest.Producer.SupervisedConnection

    @impl true
    def before_start(opts, state) do
      RabbitMQStream.Connection.create_stream(state.connection, state.stream_name)

      %{state | connection: Keyword.fetch!(opts, :custom_connection)}
    end
  end

  # SupervisedConnection is registered under a fixed name, so the next
  # test's start_link races the previous test process's implicit
  # linked-process teardown unless we stop it synchronously first. on_exit
  # runs in a separate process, so the pid found by whereis/1 can still die
  # from that same link teardown before GenServer.stop/1 reaches it; treat
  # that as already-stopped rather than letting it crash on_exit.
  setup do
    on_exit(fn ->
      case Process.whereis(SupervisedConnection) do
        nil ->
          :ok

        pid ->
          try do
            GenServer.stop(pid)
          catch
            :exit, _ -> :ok
          end
      end
    end)

    {:ok, _conn} =
      SupervisedConnection.start_link(host: "localhost", vhost: "/", backoff: {:exponential, initial: 10, max: 50})

    :ok = SupervisedConnection.connect()

    :ok
  end

  @stream "producer-test-01"
  @reference_name "producer-test-reference-01"
  test "should declare itself and its stream" do
    assert {:ok, _} =
             SupervisorProducer.start_link(reference_name: @reference_name, stream_name: @stream)

    SupervisedConnection.delete_stream(@stream)
  end

  @stream "producer-test-02"
  @reference_name "producer-test-reference-02"
  test "should query its sequence when declaring" do
    {:ok, _} =
      SupervisorProducer.start_link(
        reference_name: @reference_name,
        stream_name: @stream
      )

    # Single-node setup: the seed connection is already the stream's leader, so the
    # seed-reuse optimization in `RabbitMQStream.Connection.Router` should mean no
    # extra connection was opened.
    assert %{sequence: 1, connection: conn, seed_connection: conn} = :sys.get_state(Process.whereis(SupervisorProducer))
    SupervisedConnection.delete_stream(@stream)
  end

  @stream "producer-test-before-start-override"
  @reference_name "producer-test-reference-before-start-override"
  test "before_start/2's connection choice is not overridden by leader resolution" do
    {:ok, custom_connection} = RabbitMQStream.Connection.start_link(host: "localhost", vhost: "/")
    :ok = RabbitMQStream.Connection.connect(custom_connection)

    assert {:ok, _} =
             CustomRoutingProducer.start_link(
               reference_name: @reference_name,
               stream_name: @stream,
               custom_connection: custom_connection
             )

    assert %{connection: ^custom_connection} = :sys.get_state(Process.whereis(CustomRoutingProducer))

    SupervisedConnection.delete_stream(@stream)
  end

  @stream "producer-test-03"
  @reference_name "producer-test-reference-03"
  test "should publish a message" do
    {:ok, _} =
      SupervisorProducer.start_link(
        reference_name: @reference_name,
        stream_name: @stream
      )

    %{sequence: sequence} = :sys.get_state(Process.whereis(SupervisorProducer))

    SupervisorProducer.publish(inspect(%{message: "Hello, world!"}))

    sequence = sequence + 1

    assert %{sequence: ^sequence} = :sys.get_state(Process.whereis(SupervisorProducer))

    SupervisorProducer.publish(inspect(%{message: "Hello, world2!"}))

    sequence = sequence + 1

    assert %{sequence: ^sequence} = :sys.get_state(Process.whereis(SupervisorProducer))

    SupervisedConnection.delete_stream(@stream)
  end

  @stream "producer-test-04"
  @reference_name "producer-test-reference-04"
  test "should notify handle_confirm/1 with confirmed publishing_ids" do
    Process.register(self(), :producer_confirm_test_listener)

    {:ok, _} =
      ConfirmingProducer.start_link(
        reference_name: @reference_name,
        stream_name: @stream
      )

    ConfirmingProducer.publish(inspect(%{message: "Hello, world!"}))

    assert_confirmed()

    SupervisedConnection.delete_stream(@stream)
  end

  @stream "producer-test-05"
  @reference_name "producer-test-reference-05"
  test "redeclares itself and keeps publishing after its connection recovers from a drop" do
    Process.register(self(), :producer_confirm_test_listener)

    {:ok, _} =
      ConfirmingProducer.start_link(
        reference_name: @reference_name,
        stream_name: @stream
      )

    ConfirmingProducer.publish(inspect(%{message: "before the drop"}))
    assert_confirmed()

    %{connection: conn} = :sys.get_state(Process.whereis(ConfirmingProducer))
    %Connection{socket: socket} = :sys.get_state(conn)

    send(conn, {:tcp_closed, socket})

    # Blocks until the connection reopens (see connection_test.exs), which happens
    # strictly after this producer's `{:connection_reconnected}` notification was
    # already sent (handler.ex notifies before replying to `connect/1` callers).
    assert :ok = SupervisedConnection.connect(5_000)

    # Proves the producer actually redeclared itself against the new session (not
    # just that local state changed): a stale, pre-drop producer_id would not confirm.
    ConfirmingProducer.publish(inspect(%{message: "after the drop"}))
    assert_confirmed()

    SupervisedConnection.delete_stream(@stream)
  end

  @stream "producer-test-06"
  @reference_name "producer-test-reference-06"
  test "holds messages published while reconnecting and sends them once redeclared" do
    Process.register(self(), :producer_confirm_test_listener)

    {:ok, producer} = ConfirmingProducer.start_link(reference_name: @reference_name, stream_name: @stream)

    ConfirmingProducer.publish(inspect(%{message: "before the drop"}))
    assert_confirmed()

    %{connection: conn} = :sys.get_state(producer)
    %Connection{socket: socket} = :sys.get_state(conn)

    send(conn, {:tcp_closed, socket})
    # Processed right after the drop, before its first reconnect attempt fires: keeps it reconnecting.
    :sys.suspend(conn)

    assert %RabbitMQStream.Producer{id: nil} = :sys.get_state(producer)

    ConfirmingProducer.publish(inspect(%{message: "while reconnecting"}))
    assert :queue.len(:sys.get_state(producer).pending) == 1
    refute_receive {:confirmed, _}, 200

    :sys.resume(conn)
    assert :ok = SupervisedConnection.connect(5_000)

    assert_confirmed()
    assert :queue.is_empty(:sys.get_state(producer).pending)

    SupervisedConnection.delete_stream(@stream)
  end

  @stream "producer-test-07"
  @reference_name "producer-test-reference-07"
  test "sends several held messages in order, under the next publishing ids, once redeclared" do
    # Reads the stream back from :first, so leftovers from a failed run would break it.
    SupervisedConnection.delete_stream(@stream)
    Process.register(self(), :producer_confirm_test_listener)

    {:ok, producer} = ConfirmingProducer.start_link(reference_name: @reference_name, stream_name: @stream)

    ConfirmingProducer.publish("before the drop")
    [before_id] = collect_confirms(1)

    %{connection: conn} = :sys.get_state(producer)
    %Connection{socket: socket} = :sys.get_state(conn)

    send(conn, {:tcp_closed, socket})
    :sys.suspend(conn)

    held = ["held 1", "held 2", "held 3"]
    for message <- held, do: ConfirmingProducer.publish(message)
    assert :queue.len(:sys.get_state(producer).pending) == 3

    :sys.resume(conn)
    assert :ok = SupervisedConnection.connect(5_000)

    # Ids at or below the broker's last sequence would be deduplicated away.
    assert collect_confirms(3) == Enum.to_list((before_id + 1)..(before_id + 3))

    # A confirm alone doesn't prove the message was stored, so read the stream back.
    {:ok, _} = SupervisedConnection.subscribe(@stream, self(), :first, 10)
    assert collect_entries(4) == ["before the drop" | held]

    SupervisedConnection.delete_stream(@stream)
  end

  @stream "producer-test-08"
  @reference_name "producer-test-reference-08"
  test "stops, dropping held messages, when it can't redeclare itself after a reconnect" do
    Process.flag(:trap_exit, true)
    SupervisedConnection.delete_stream(@stream)

    {:ok, producer} = ConfirmingProducer.start_link(reference_name: @reference_name, stream_name: @stream)
    monitor = Process.monitor(producer)

    %{connection: conn} = :sys.get_state(producer)
    %Connection{socket: socket} = :sys.get_state(conn)

    send(conn, {:tcp_closed, socket})
    :sys.suspend(conn)

    ConfirmingProducer.publish("held")

    # Deleted while reconnecting, so the redeclare fails.
    {:ok, other} = Connection.start_link(host: "localhost", vhost: "/")
    :ok = Connection.connect(other)
    :ok = Connection.delete_stream(other, @stream)
    GenServer.stop(other)

    log =
      capture_log(fn ->
        :sys.resume(conn)
        assert :ok = SupervisedConnection.connect(5_000)
        assert_receive {:DOWN, ^monitor, :process, ^producer, {:error, _}}, 5_000
      end)

    assert log =~ "dropping 1 held messages"
  end

  defp collect_confirms(count, acc \\ [])
  defp collect_confirms(count, acc) when length(acc) >= count, do: Enum.sort(acc)

  defp collect_confirms(count, acc) do
    receive do
      {:confirmed, ids} -> collect_confirms(count, acc ++ ids)
      {:publish_error, errors} -> flunk("Publish rejected by the broker: #{inspect(errors)}")
    after
      5_000 -> flunk("Got #{length(acc)} of #{count} confirms within 5s")
    end
  end

  defp collect_entries(count, acc \\ [])
  defp collect_entries(count, acc) when length(acc) >= count, do: acc

  defp collect_entries(count, acc) do
    receive do
      {:deliver, %{osiris_chunk: %{data_entries: entries}}} -> collect_entries(count, acc ++ entries)
    after
      5_000 -> flunk("Read #{length(acc)} of #{count} messages within 5s: #{inspect(acc)}")
    end
  end
end

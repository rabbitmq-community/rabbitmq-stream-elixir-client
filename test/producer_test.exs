defmodule RabbitMQStreamTest.Producer do
  use ExUnit.Case, async: false

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

    {:ok, _conn} = SupervisedConnection.start_link(host: "localhost", vhost: "/")
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

    assert %{sequence: 1} = :sys.get_state(Process.whereis(SupervisorProducer))
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
end

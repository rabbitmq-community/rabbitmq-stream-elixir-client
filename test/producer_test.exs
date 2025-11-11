defmodule RabbitMQStreamTest.Producer do
  use ExUnit.Case, async: false

  @moduletag :v3_11
  @moduletag :v3_12
  @moduletag :v3_13

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

  setup do
    {:ok, conn} = SupervisedConnection.start_link(host: "localhost", vhost: "/")
    :ok = SupervisedConnection.connect()

    [conn: conn]
  end

  @stream "producer-test-01"
  @reference_name "producer-test-reference-01"
  test "should declare itself and its stream", %{conn: conn} do
    assert {:ok, _} =
             SupervisorProducer.start_link(reference_name: @reference_name, stream_name: @stream)

    RabbitMQStream.Connection.delete_stream(conn, @stream)
  end

  @stream "producer-test-02"
  @reference_name "producer-test-reference-02"
  test "should query its sequence when declaring", %{conn: conn} do
    {:ok, _} =
      SupervisorProducer.start_link(
        reference_name: @reference_name,
        stream_name: @stream
      )

    assert %{sequence: 1} = :sys.get_state(Process.whereis(SupervisorProducer))
    RabbitMQStream.Connection.delete_stream(conn, @stream)
  end

  @stream "producer-test-03"
  @reference_name "producer-test-reference-03"
  test "should publish a message", %{conn: conn} do
    {:ok, _} =
      SupervisorProducer.start_link(
        connection: conn,
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

    RabbitMQStream.Connection.delete_stream(conn, @stream)
  end
end

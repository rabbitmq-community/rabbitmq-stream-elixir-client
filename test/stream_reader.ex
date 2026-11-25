defmodule RabbitMQStreamTest.StreamReader do
  use ExUnit.Case, async: false

  @moduletag :v3_11
  @moduletag :v3_12
  @moduletag :v3_13

  defmodule SupervisedConnection do
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

  setup do
    {:ok, _conn} = SupervisedConnection.start_link(host: "localhost", vhost: "/")
    :ok = SupervisedConnection.connect()

    :ok
  end

  @stream "stream-reader-01"
  test "should create a stream" do
    SupervisedConnection.delete_stream(@stream)

    {:ok, _producer} =
      SupervisorProducer.start_link(stream_name: @stream)

    for i <- 0..100_000 do
      SupervisorProducer.publish("#{i}")
    end

    stream =
      RabbitMQStream.StreamReader.stream!(
        connection: SupervisedConnection,
        stream_name: @stream,
        batch_size: 5,
        offset: :first
      )

    sum =
      stream
      |> Stream.map(&String.to_integer/1)
      |> Stream.take(100_000)
      |> Enum.reduce(0, &+/2)

    dbg(sum)
    :erlang.process_info(Process.whereis(SupervisedConnection), :messages) |> dbg()
    SupervisedConnection.delete_stream(@stream)
  end
end

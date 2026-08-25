defmodule RabbitMQStreamTest.Interop do
  use ExUnit.Case, async: false

  @moduletag :v3_11
  @moduletag :v3_12
  @moduletag :v3_13
  @moduletag :v4_2
  @moduletag :v4_3

  defmodule MyConnection do
    use RabbitMQStream.Connection
  end

  defmodule MyConsumer do
    use RabbitMQStream.Consumer,
      connection: MyConnection

    @impl true
    def handle_message(message, %{private: parent}) do
      send(parent, {:message, message})

      :ok
    end

    @impl true
    def decode!(message) do
      :amqp10_framing.decode_bin(message)[:"v1_0.data"]
    end
  end

  setup do
    {:ok, _conn} = MyConnection.start_link(host: "localhost", vhost: "/")
    :ok = MyConnection.connect()

    :ok
  end

  @exchange "interop-exchange"
  @stream "interop-stream"
  test "should consume from a stream that was declared by amqp" do
    {:ok, conn} = AMQP.Connection.open()
    {:ok, channel} = AMQP.Channel.open(conn)

    # Since we are reading from ':first', we should make sure the queue is empty
    AMQP.Queue.delete(channel, @stream)

    {:ok, _} =
      AMQP.Queue.declare(
        channel,
        @stream,
        durable: true,
        arguments: [{"x-queue-type", "stream"}]
      )

    :ok = AMQP.Exchange.declare(channel, @exchange)
    AMQP.Queue.bind(channel, @stream, @exchange)

    message = "Hello, World!"

    :ok = AMQP.Basic.publish(channel, @exchange, "", message)

    {:ok, _consumer} =
      MyConsumer.start_link(
        initial_offset: :first,
        stream_name: @stream,
        private: self(),
        offset_tracking: [count: [store_after: 1]]
      )

    # We are pattern matching with the content itself because we have already decoded
    #  the message from the `:amqp` binary format, in the `decode!/1` callback defined
    #  in the `MyConsumer`
    assert_receive {:message, ^message}, 1000
  end
end

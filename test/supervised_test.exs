defmodule RabbitMQStreamTest.SupervisedTest do
  # This module ensure that all the Modules must be able to be started using `GenServer.start_link`
  # without having to necessarily declare it inside a module.
  use ExUnit.Case, async: false

  @moduletag :v3_11
  @moduletag :v3_12
  @moduletag :v3_13
  @moduletag :v4_2
  @moduletag :v4_3

  defmodule ConsumerModule do
    # We need to define a module to receive the 'handle_message' calls, but itself doesn't need
    # to be `use RabbitMQStream.Consumer`

    def handle_message(entry, %{private: parent}) do
      send(parent, {:message, entry})

      :ok
    end
  end

  @stream "consumer-test-stream-01"
  test "should start each module" do
    assert {:ok, conn} = RabbitMQStream.Connection.start_link(host: "localhost", vhost: "/"),
           "Should start a connection"

    assert :ok = RabbitMQStream.Connection.connect(conn),
           "Should connect to it"

    :ok = RabbitMQStream.Connection.create_stream(conn, @stream)

    assert {:ok, producer} =
             RabbitMQStream.Producer.start_link(
               connection: conn,
               stream_name: @stream
             ),
           "Should start a producer"

    assert {:ok, _consumer} =
             RabbitMQStream.Consumer.start_link(
               connection: conn,
               stream_name: @stream,
               consumer_module: ConsumerModule,
               private: self(),
               initial_offset: :first
             ),
           "Should start a consumer"

    # Publish to producer
    :ok = RabbitMQStream.Producer.publish(producer, "Hello, World!")

    assert_receive {:message, "Hello, World!"}, 5000
  end
end

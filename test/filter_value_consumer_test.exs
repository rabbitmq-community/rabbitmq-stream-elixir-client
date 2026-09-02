defmodule RabbitMQStreamTest.Consumer.FilterValue do
  use ExUnit.Case, async: false

  @moduletag :v3_13
  @moduletag :v4_2
  @moduletag :v4_3

  defmodule Conn1 do
    use RabbitMQStream.Connection
  end

  defmodule Producer do
    use RabbitMQStream.Producer,
      connection: Conn1,
      serializer: Jason

    @impl true
    def filter_value(message) do
      message["key"]
    end
  end

  defmodule Subs1 do
    use RabbitMQStream.Consumer,
      connection: Conn1,
      initial_offset: :next,
      stream_name: "filter-value-01",
      offset_tracking: [count: [store_after: 1]],
      properties: [filter: ["Subs1"]],
      serializer: Jason

    @impl true
    def handle_message(entry, %{private: parent}) do
      send(parent, {__MODULE__, entry})

      :ok
    end

    @impl true
    def filter_value(message) do
      message["key"]
    end
  end

  defmodule Subs2 do
    use RabbitMQStream.Consumer,
      connection: Conn1,
      initial_offset: :next,
      stream_name: "filter-value-01",
      offset_tracking: [count: [store_after: 1]],
      properties: [filter: ["Subs2"]],
      serializer: Jason

    @impl true
    def handle_message(entry, %{private: parent}) do
      send(parent, {__MODULE__, entry})

      :ok
    end

    @impl true
    def filter_value(message) do
      message["key"]
    end
  end

  defmodule Subs3 do
    use RabbitMQStream.Consumer,
      connection: Conn1,
      initial_offset: :next,
      stream_name: "filter-value-01",
      offset_tracking: [count: [store_after: 1]],
      properties: [match_unfiltered: true],
      serializer: Jason

    @impl true
    def handle_message(entry, %{private: parent}) do
      send(parent, {__MODULE__, entry})

      :ok
    end

    @impl true
    def filter_value(message) do
      message["key"]
    end
  end

  test "should receive only the filtered messages" do
    {:ok, _} = Conn1.start_link()
    :ok = Conn1.connect()
    Conn1.delete_stream("filter-value-01")
    :ok = Conn1.create_stream("filter-value-01")
    {:ok, _} = Producer.start_link(stream_name: "filter-value-01")

    {:ok, _} = Subs1.start_link(private: self())
    {:ok, _} = Subs2.start_link(private: self())
    {:ok, _} = Subs3.start_link(private: self())

    # start_link/1 returns as soon as init/1 does, before the subscribe request (sent
    # from handle_continue/2) is acknowledged by the broker. Since GenServer guarantees
    # handle_continue completes before any queued call is answered, these calls block
    # until each subscription is active, avoiding a race with `publish` below (with
    # `initial_offset: :next`, anything published before a subscription is active is
    # simply never delivered to it).
    Subs1.get_credits()
    Subs2.get_credits()
    Subs3.get_credits()

    message1 = %{"key" => "Subs1", "value" => "1"}
    message2 = %{"key" => "Subs2", "value" => "2"}
    message3 = %{"value" => "4"}

    :ok = Producer.publish(message1)
    :ok = Producer.publish(message2)
    :ok = Producer.publish(message3)

    assert_receive {Subs1, ^message1}, 500
    assert_receive {Subs2, ^message2}, 500
    assert_receive {Subs3, ^message3}, 500
  end
end

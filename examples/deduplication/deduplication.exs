# Message deduplication: the broker drops a publish whose publishing_id it has already seen
# for a given reference_name, so retries after a crash/timeout are safe. This example uses the
# low-level `Connection` API to resend a publishing_id on purpose, then reads the stream back to
# show the duplicate never landed. See https://blog.rabbitmq.com/posts/2021/07/rabbitmq-streams-message-deduplication/
#
# Run with:
#   mix run examples/deduplication/deduplication.exs

stream_name = "deduplication-example-stream"
reference_name = "deduplication-example-producer"

defmodule Deduplication.Connection do
  use RabbitMQStream.Connection
end

defmodule Deduplication.Consumer do
  use RabbitMQStream.Consumer,
    connection: Deduplication.Connection,
    stream_name: stream_name,
    initial_offset: :first

  @impl true
  def handle_message(message) do
    send(:deduplication_example, {:received, message})
    :ok
  end
end

Process.register(self(), :deduplication_example)

{:ok, _connection} = Deduplication.Connection.start_link()

case Deduplication.Connection.create_stream(stream_name) do
  :ok -> :ok
  {:error, :stream_already_exists} -> :ok
end

{:ok, producer_id} = Deduplication.Connection.declare_producer(stream_name, reference_name)

:ok = Deduplication.Connection.publish(producer_id, 1, "eu")
:ok = Deduplication.Connection.publish(producer_id, 2, "latam")
:ok = Deduplication.Connection.publish(producer_id, 3, "apac")
# Resent as if we weren't sure the earlier publish of publishing_id 2 made it through.
:ok = Deduplication.Connection.publish(producer_id, 2, "latam")

Process.sleep(500)

{:ok, _consumer} = Deduplication.Consumer.start_link()

Process.sleep(1_000)

# Sleeping first keeps this simplistic: by now all messages are already sitting in the mailbox.
messages =
  Stream.repeatedly(fn ->
    receive do
      {:received, message} -> message
    after
      0 -> nil
    end
  end)
  |> Enum.take_while(&(&1 != nil))

IO.puts(
  "4 publishes were sent (publishing_id 2 reused as a duplicate), stream has #{length(messages)}: #{inspect(messages)}"
)

Deduplication.Connection.delete_producer(producer_id)
Deduplication.Connection.delete_stream(stream_name)

# Automatic reconnection: a dropped TCP/TLS connection is retried with backoff, and every
# producer/consumer attached to it is transparently redeclared/resubscribed once it reopens --
# no restart, no lost publisher/subscription identity. Enabled by default; set `reconnect: false`
# on the Connection to opt out instead (a drop then stops the connection process with
# `{:shutdown, {:connection_closed, reason}}`, for a supervisor to restart).
#
# This example publishes and consumes continuously so you can watch recovery happen live. Start
# it, then actually break the connection some way, e.g.:
#   - restart the broker: `docker compose -f services/docker-compose.yaml restart`
#   - or drop just this client's TCP connection without touching the broker, e.g.
#     `sudo lsof -ti tcp:5552 -sTCP:ESTABLISHED | xargs -I{} sudo kill -9 {}` (closes the
#     socket from the OS level; the broker sees a normal disconnect)
#
# Watch the "Connection state" line flip to `reconnecting` and back to `open`, and note that
# messages published *while* disconnected are not delivered (`publish/3` is fire-and-forget and
# the in-flight publisher id becomes stale mid-drop -- this is a known, documented gap, tracked
# alongside Priority 3's backpressure work) -- but publishing resumes normally once reconnected.
#
# Terminate the example itself with Ctrl+C (this is a plain `mix run` script, so like any other
# long-running Elixir/Erlang program, Ctrl+C brings up the VM's BREAK menu -- press `a` + Enter,
# or Ctrl+C a second time, to actually exit).
#
# Run with:
#   mix run examples/auto_reconnection/auto_reconnection.exs

stream_name = "auto-reconnection-example-stream"

defmodule AutoReconnection.Connection do
  use RabbitMQStream.Connection
end

defmodule AutoReconnection.Consumer do
  use RabbitMQStream.Consumer,
    connection: AutoReconnection.Connection,
    stream_name: stream_name,
    initial_offset: :next

  @impl true
  def handle_message(message) do
    IO.puts("Received: #{message}")
    :ok
  end
end

defmodule AutoReconnection.Producer do
  use RabbitMQStream.Producer,
    connection: AutoReconnection.Connection,
    stream_name: stream_name
end

# Polls the connection's state and prints it whenever it changes, so a drop/reconnect cycle is
# visible without needing a dedicated state-change listener (not implemented by the client yet).
defmodule AutoReconnection.StateWatcher do
  def start(connection) do
    spawn_link(fn -> loop(connection, nil) end)
  end

  defp loop(connection, last_state) do
    state = :sys.get_state(connection).state

    if state != last_state do
      IO.puts("Connection state: #{state}")
    end

    Process.sleep(100)
    loop(connection, state)
  end
end

{:ok, connection} = AutoReconnection.Connection.start_link()

case AutoReconnection.Connection.create_stream(stream_name) do
  :ok -> :ok
  {:error, :stream_already_exists} -> :ok
end

AutoReconnection.StateWatcher.start(connection)

{:ok, _producer} = AutoReconnection.Producer.start_link()
{:ok, _consumer} = AutoReconnection.Consumer.start_link()

IO.puts("Publishing once a second. Break the connection to watch it recover. Ctrl+C to stop.\n")

Stream.interval(1_000)
|> Stream.each(fn i -> AutoReconnection.Producer.publish("message #{i}") end)
|> Stream.run()

defmodule RabbitMQStreamTest.ConnectionReconnect do
  use ExUnit.Case, async: true

  alias RabbitMQStream.Connection
  alias RabbitMQStream.Connection.{Backoff, Lifecycle}

  @moduletag :v3_11
  @moduletag :v3_12
  @moduletag :v3_13
  @moduletag :v4_2
  @moduletag :v4_3

  # A transport double whose `close/1` signals back to the calling process (which,
  # since these tests call `Lifecycle`'s callbacks directly rather than through a
  # live GenServer, is the test process itself) -- lets tests assert the socket was
  # actually closed without needing a real socket.
  defmodule FakeTransport do
    import Kernel, except: [send: 2]

    @behaviour RabbitMQStream.Connection.Transport
    def connect(_options), do: {:error, :econnrefused}

    def close(socket) do
      Kernel.send(self(), {:closed, socket})
      :ok
    end

    def send(_socket, _data), do: :ok
  end

  defp conn(opts) do
    struct(
      %Connection{transport: FakeTransport, options: [reconnect: true, host: "localhost", port: 5552]},
      opts
    )
  end

  test "a drop from :open moves to :reconnecting, captures targets, clears maps, and fails in-flight requests" do
    ref = make_ref()
    from = {self(), ref}

    conn =
      conn(
        state: :open,
        socket: :fake_socket,
        subscriptions: %{1 => self()},
        producers: %{2 => self()},
        request_tracker: %{{:query_offset, 1} => {from, nil}},
        backoff: Backoff.init(:exponential)
      )

    assert {:noreply, new_conn} = Lifecycle.handle_info({:tcp_closed, :fake_socket}, conn)

    assert new_conn.state == :reconnecting
    assert new_conn.reconnect_targets == [self()]
    assert new_conn.subscriptions == %{}
    assert new_conn.producers == %{}
    assert new_conn.socket == nil
    assert new_conn.reconnect_attempts == 1

    assert_receive {:closed, :fake_socket}
    assert_receive {^ref, {:error, {:closed, :tcp_closed}}}
  end

  test "a drop with reconnect: false stops the connection in an orderly way" do
    ref = make_ref()
    from = {self(), ref}

    conn =
      conn(
        state: :open,
        socket: :fake_socket,
        options: [reconnect: false],
        connect_requests: [from],
        backoff: Backoff.init(:exponential)
      )

    assert {:stop, {:shutdown, {:connection_closed, :tcp_closed}}, final_conn} =
             Lifecycle.handle_info({:tcp_closed, :fake_socket}, conn)

    assert final_conn.connect_requests == []
    assert_receive {:closed, :fake_socket}
    assert_receive {^ref, {:error, :closed}}
  end

  test "a drop while :closing falls through to the existing close teardown, not :reconnecting" do
    ref = make_ref()
    from = {self(), ref}

    conn =
      conn(
        state: :closing,
        socket: :fake_socket,
        connect_requests: [from],
        backoff: Backoff.init(:exponential)
      )

    assert {:noreply, final_conn, :hibernate} = Lifecycle.handle_info({:tcp_closed, :fake_socket}, conn)

    assert final_conn.state == :closed
    assert_receive {:closed, :fake_socket}
    assert_receive {^ref, {:error, :closed}}
  end

  # Bug 2 regression: `handle_closing/1` used to guard the socket close with
  # `is_port/1`, which is false for `:ssl` sockets (`{:sslsocket, ...}` tuples), so
  # TLS connections leaked on an orderly close. A non-port socket value here stands
  # in for that shape -- the close must still happen unconditionally.
  test "the socket is closed unconditionally, even when it isn't a port (TLS shape)" do
    conn =
      conn(
        state: :closing,
        socket: {:sslsocket, :fake, :fake},
        backoff: Backoff.init(:exponential)
      )

    assert {:noreply, _final_conn, :hibernate} =
             Lifecycle.handle_info({:ssl_closed, {:sslsocket, :fake, :fake}}, conn)

    assert_receive {:closed, {:sslsocket, :fake, :fake}}
  end

  test "reconnect_targets accumulate across a second drop before recovery completes" do
    conn =
      conn(
        state: :open,
        socket: :fake_socket,
        subscriptions: %{1 => self()},
        backoff: Backoff.init(:exponential)
      )

    assert {:noreply, reconnecting_conn} = Lifecycle.handle_info({:tcp_closed, :fake_socket}, conn)
    assert reconnecting_conn.reconnect_targets == [self()]
    assert_receive {:closed, :fake_socket}

    # A second drop happens before the connection ever reaches :open again -- e.g. a
    # low-level reconnect attempt got the socket partway open before dropping again.
    second_drop = %{reconnecting_conn | state: :connecting, socket: :second_socket}

    assert {:noreply, still_reconnecting} = Lifecycle.handle_info({:tcp_closed, :second_socket}, second_drop)

    assert still_reconnecting.reconnect_targets == [self()]
    assert_receive {:closed, :second_socket}
  end

  test "a stale :reconnect_attempt message, from before the attempt counter moved on, is a no-op" do
    conn = conn(state: :reconnecting, reconnect_attempts: 2, backoff: Backoff.init(:exponential))

    assert {:noreply, ^conn} = Lifecycle.handle_info({:reconnect_attempt, 1}, conn)
  end

  test "a failed reconnect attempt reschedules with an incremented attempt count" do
    conn =
      conn(
        state: :reconnecting,
        reconnect_attempts: 1,
        backoff: Backoff.init({:exponential, initial: 1, max: 5})
      )

    assert {:noreply, new_conn} = Lifecycle.handle_info({:reconnect_attempt, 1}, conn)

    assert new_conn.state == :reconnecting
    assert new_conn.reconnect_attempts == 2
  end
end

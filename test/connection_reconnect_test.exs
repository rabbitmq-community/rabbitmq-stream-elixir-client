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

  test "a drop tells attached producers/consumers right away, not only once it reopens" do
    consumer = spawn(fn -> Process.sleep(:infinity) end)
    producer = self()

    conn =
      conn(
        state: :open,
        socket: :fake_socket,
        subscriptions: %{1 => consumer},
        producers: %{2 => producer},
        backoff: Backoff.init(:exponential)
      )

    :erlang.trace(consumer, true, [:receive])
    assert {:noreply, _new_conn} = Lifecycle.handle_info({:tcp_closed, :fake_socket}, conn)

    assert_receive {:connection_lost}
    assert_receive {:trace, ^consumer, :receive, {:connection_lost}}
  end

  test "publish/credit sent with a dropped session's ids are discarded, not replayed after reopen" do
    conn = conn(state: :reconnecting, reconnect_attempts: 1, backoff: Backoff.init(:exponential))

    assert {:noreply, ^conn} = Lifecycle.handle_cast({:publish, producer_id: 1, publishing_id: 5}, conn)
    assert {:noreply, ^conn} = Lifecycle.handle_cast({:credit, subscription_id: 1, credit: 1}, conn)

    # Before the first open there's no dropped session: these are still queued as before.
    fresh = conn(state: :connecting, backoff: Backoff.init(:exponential))
    assert {:noreply, queued} = Lifecycle.handle_cast({:credit, subscription_id: 1, credit: 1}, fresh)
    assert :queue.len(queued.request_buffer) == 1
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

  test "a heartbeat tick from a dropped session is ignored instead of writing to a dead socket" do
    conn = conn(state: :reconnecting, socket: nil, reconnect_attempts: 1, backoff: Backoff.init(:exponential))

    assert {:noreply, ^conn} = Lifecycle.handle_info({:heartbeat, :old_socket}, conn)
  end

  test "late events from an already-replaced socket don't touch the current session" do
    conn = conn(state: :connecting, socket: :new_socket, reconnect_attempts: 1, backoff: Backoff.init(:exponential))

    assert {:noreply, ^conn} = Lifecycle.handle_info({:tcp_closed, :old_socket}, conn)
    assert {:noreply, ^conn} = Lifecycle.handle_info({:tcp_error, :old_socket, :econnreset}, conn)
    assert {:noreply, ^conn} = Lifecycle.handle_info({:tcp, :old_socket, <<0, 0, 0, 1>>}, conn)
    refute_received {:closed, _}
  end

  test "a drop during a reconnect attempt's handshake keeps counting attempts" do
    conn =
      conn(
        state: :connecting,
        socket: :retry_socket,
        reconnect_attempts: 3,
        backoff: Backoff.init(:exponential)
      )

    assert {:noreply, new_conn} = Lifecycle.handle_info({:tcp_closed, :retry_socket}, conn)

    assert new_conn.state == :reconnecting
    assert new_conn.reconnect_attempts == 4
  end

  test "a reconnect handshake refused by the broker is retried rather than abandoning recovery" do
    ref = make_ref()

    conn =
      conn(
        state: :opening,
        socket: :retry_socket,
        reconnect_attempts: 2,
        reconnect_targets: [self()],
        connect_requests: [{self(), ref}],
        backoff: Backoff.init(:exponential)
      )

    # sasl_authenticate response (0x8013), version 1, correlation id 1, code authentication_failure (0x08).
    payload = <<0x8013::16, 1::16, 1::32, 0x08::16>>
    frame = <<byte_size(payload)::32, payload::binary>>

    assert {:noreply, new_conn} = Lifecycle.handle_info({:tcp, :retry_socket, frame}, conn)

    assert new_conn.state == :reconnecting
    assert new_conn.reconnect_attempts == 3
    assert new_conn.reconnect_targets == [self()]
    assert new_conn.socket == nil
    assert new_conn.connect_requests == []
    assert_receive {:closed, :retry_socket}
    assert_receive {^ref, {:error, :authentication_failure}}
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

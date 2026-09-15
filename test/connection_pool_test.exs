defmodule RabbitMQStreamTest.ConnectionPool do
  use ExUnit.Case, async: false

  alias RabbitMQStream.Connection.Pool

  @moduletag :v3_11
  @moduletag :v3_12
  @moduletag :v3_13
  @moduletag :v4_2
  @moduletag :v4_3

  @tcp_options [vhost: "/", username: "guest", password: "guest"]
  @ssl_options [
    vhost: "/",
    username: "guest",
    password: "guest",
    transport: :ssl,
    ssl_opts: [
      keyfile: "services/cert/client_box_key.pem",
      certfile: "services/cert/client_box_certificate.pem",
      cacertfile: "services/cert/ca_certificate.pem",
      verify: :verify_peer
    ]
  ]

  test "reuses the same connection for the same key" do
    assert {:ok, pid1} = Pool.get_or_start_connection(@tcp_options, "localhost", 5552)
    assert {:ok, pid2} = Pool.get_or_start_connection(@tcp_options, "localhost", 5552)

    assert pid1 == pid2
    assert %RabbitMQStream.Connection{state: :open} = :sys.get_state(pid1)
  end

  test "opens a distinct connection for a different transport/port, even on the same host" do
    assert {:ok, tcp_pid} = Pool.get_or_start_connection(@tcp_options, "localhost", 5552)
    assert {:ok, ssl_pid} = Pool.get_or_start_connection(@ssl_options, "localhost", 5551)

    assert tcp_pid != ssl_pid
    assert %RabbitMQStream.Connection{state: :open} = :sys.get_state(ssl_pid)
  end

  test "opens a distinct connection when only ssl_opts differ, even with the same transport" do
    assert {:ok, pid1} = Pool.get_or_start_connection(@tcp_options, "localhost", 5552)

    # `ssl_opts` is inert for `transport: :tcp` -- this isolates "does the key differ"
    # from "does the connection actually behave differently".
    other_options = Keyword.put(@tcp_options, :ssl_opts, verify: :verify_none)
    assert {:ok, pid2} = Pool.get_or_start_connection(other_options, "localhost", 5552)

    assert pid1 != pid2
    assert %RabbitMQStream.Connection{state: :open} = :sys.get_state(pid2)
  end

  test "attempts its own authentication instead of reusing another caller's session when only the password differs" do
    assert {:ok, pid1} = Pool.get_or_start_connection(@tcp_options, "localhost", 5552)

    wrong_password_options = Keyword.put(@tcp_options, :password, "definitely-wrong-password")
    assert {:error, :authentication_failure} = Pool.get_or_start_connection(wrong_password_options, "localhost", 5552)

    # The first caller's already-open connection must be untouched -- before this
    # fix, the second call would have resolved to the same pool key and returned
    # `{:ok, pid1}` without ever attempting its own authentication.
    assert %RabbitMQStream.Connection{state: :open} = :sys.get_state(pid1)
  end

  test "redials after the pooled connection process dies" do
    assert {:ok, pid1} = Pool.get_or_start_connection(@tcp_options, "localhost", 5552)

    ref = Process.monitor(pid1)
    Process.exit(pid1, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid1, :killed}

    assert {:ok, pid2} = Pool.get_or_start_connection(@tcp_options, "localhost", 5552)

    assert pid2 != pid1
    assert %RabbitMQStream.Connection{state: :open} = :sys.get_state(pid2)
  end

  # Note: true multi-node reuse (two different real broker nodes, each getting its own
  # pooled connection) isn't meaningfully testable with the single-node compose used
  # here — that's covered by the cluster integration test in clustered_test.exs instead.

  test "fails fast against an unreachable address instead of hanging" do
    # 192.0.2.0/24 is IANA's reserved TEST-NET-1 block (RFC 5737) -- never assigned or
    # routed anywhere, so this reliably simulates a dropped-SYN/unreachable leader
    # address without depending on any specific network's firewall behavior.
    opts = Keyword.put(@tcp_options, :connect_timeout, 2_000)

    start = System.monotonic_time(:millisecond)
    result = Pool.get_or_start_connection(opts, "192.0.2.1", 5552)
    elapsed = System.monotonic_time(:millisecond) - start

    assert {:error, _reason} = result
    refute match?({:error, :noproc}, result)
    assert elapsed < 10_000
  end
end

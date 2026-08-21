defmodule RabbitMQStreamTest.ConnectionPool do
  use ExUnit.Case, async: false

  alias RabbitMQStream.Connection.Pool

  @moduletag :v3_11
  @moduletag :v3_12
  @moduletag :v3_13

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
end

defmodule RabbitMQStreamTest.ConnectionRouter do
  use ExUnit.Case, async: false

  alias RabbitMQStream.Connection
  alias RabbitMQStream.Connection.Router

  @moduletag :v3_11
  @moduletag :v3_12
  @moduletag :v3_13
  @moduletag :v4_2
  @moduletag :v4_3

  @stream "connection-router-test-seed-reuse"

  setup do
    {:ok, admin} = Connection.start_link(host: "localhost", vhost: "/")
    :ok = Connection.connect(admin)
    Connection.create_stream(admin, @stream)

    on_exit(fn ->
      {:ok, admin} = Connection.start_link(host: "localhost", vhost: "/")
      :ok = Connection.connect(admin)
      Connection.delete_stream(admin, @stream)
    end)

    :ok
  end

  test "reuses the seed connection even when the dialed address differs from the broker's advertised address" do
    # Dialing via "127.0.0.1" instead of "localhost" reproduces the real-world mismatch
    # (client dials one address, broker advertises a syntactically different one for
    # the same node) without depending on Docker/hostname setup. A naive comparison of
    # the dialed address against the leader's advertised host/port from `query_metadata`
    # would see "127.0.0.1" != "localhost" and wrongly conclude the leader is some other
    # node, opening a redundant (and, on a real multi-node cluster with an unreachable
    # advertised host, potentially unreachable) pooled connection instead of reusing
    # this already-open, already-correct seed connection.
    {:ok, seed} = Connection.start_link(host: "127.0.0.1", vhost: "/")
    :ok = Connection.connect(seed)

    assert Connection.get_connection_properties(seed)["advertised_host"] != "127.0.0.1"

    assert {:ok, ^seed} = Router.producer_connection(seed, @stream)
    assert {:ok, ^seed} = Router.consumer_connection(seed, @stream)
  end
end

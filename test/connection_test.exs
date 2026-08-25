defmodule RabbitMQStreamTest.Connection do
  use ExUnit.Case, async: false
  alias RabbitMQStream.Connection
  import ExUnit.CaptureLog

  @moduletag :v3_11
  @moduletag :v3_12
  @moduletag :v3_13
  @moduletag :v4_2
  @moduletag :v4_3

  defmodule SupervisedConnection do
    use RabbitMQStream.Connection
  end

  defmodule SupervisedSSLConnection do
    use RabbitMQStream.Connection,
      port: 5551,
      transport: :ssl,
      ssl_opts: [
        keyfile: "services/cert/client_box_key.pem",
        certfile: "services/cert/client_box_certificate.pem",
        cacertfile: "services/cert/ca_certificate.pem",
        verify: :verify_peer
      ]
  end

  # SupervisedConnection/SupervisedSSLConnection are registered under a fixed
  # name, so the next test's start_link races the previous test process's
  # implicit linked-process teardown unless we stop it synchronously first.
  # on_exit runs in a separate process, so the pid found by whereis/1 can
  # still die from that same link teardown before GenServer.stop/1 reaches
  # it; treat that as already-stopped rather than letting it crash on_exit.
  setup do
    on_exit(fn ->
      for mod <- [SupervisedConnection, SupervisedSSLConnection] do
        case Process.whereis(mod) do
          nil ->
            :ok

          pid ->
            try do
              GenServer.stop(pid)
            catch
              :exit, _ -> :ok
            end
        end
      end
    end)
  end

  test "should open and close the connection" do
    {:ok, conn} = SupervisedConnection.start_link(host: "localhost", vhost: "/", lazy: true)

    assert %Connection{state: :closed} = :sys.get_state(conn)

    assert :ok = SupervisedConnection.connect()

    assert %Connection{state: :open} = :sys.get_state(conn)

    assert :ok = SupervisedConnection.close()

    assert %Connection{state: :closed} = :sys.get_state(conn)

    assert :ok = SupervisedConnection.close()
  end

  test "should open and close a ssl connection" do
    {:ok, conn} = SupervisedSSLConnection.start_link(host: "localhost", vhost: "/", lazy: true)

    assert %Connection{state: :closed} = :sys.get_state(conn)

    assert :ok = SupervisedSSLConnection.connect()

    assert %Connection{state: :open} = :sys.get_state(conn)

    assert :ok = SupervisedSSLConnection.close()

    assert %Connection{state: :closed} = :sys.get_state(conn)

    assert :ok = SupervisedSSLConnection.close()
  end

  test "should correctly answer to parallel `connect` requests" do
    {:ok, _} = SupervisedConnection.start_link(host: "localhost", vhost: "/", lazy: true)

    result =
      Task.async_stream(0..10, fn _ -> SupervisedConnection.connect() end)
      |> Enum.to_list()

    assert Enum.all?(result, &({:ok, :ok} = &1))
  end

  test "should fail to connect with expected error messages" do
    {:ok, pid} = SupervisedConnection.start_link(host: "localhost", vhost: "/NONEXISTENT", lazy: true)

    assert capture_log(fn -> assert {:error, :virtual_host_access_failure} = SupervisedConnection.connect() end) =~
             "Failed to connect"

    :ok = GenServer.stop(pid)

    {:ok, _} =
      SupervisedConnection.start_link(host: "localhost", vhost: "/", user: "guest", password: "wrong", lazy: true)

    assert capture_log(fn -> assert {:error, :authentication_failure} = SupervisedConnection.connect() end) =~
             "Failed to connect"
  end

  test "should create and delete a stream" do
    {:ok, _} = SupervisedConnection.start_link(host: "localhost", vhost: "/")
    :ok = SupervisedConnection.connect()

    SupervisedConnection.delete_stream("test-create-01")
    assert :ok = SupervisedConnection.create_stream("test-create-01")
    assert {:error, :stream_already_exists} = SupervisedConnection.create_stream("test-create-01")

    assert :ok = SupervisedConnection.delete_stream("test-create-01")
    assert {:error, :stream_does_not_exist} = SupervisedConnection.delete_stream("test-create-01")
    SupervisedConnection.close()
  end

  @stream "test-store-03"
  test "should store and query an offset" do
    {:ok, _} = SupervisedConnection.start_link(host: "localhost", vhost: "/")
    :ok = SupervisedConnection.connect()

    SupervisedConnection.delete_stream(@stream)
    :ok = SupervisedConnection.create_stream(@stream)

    offset = :os.system_time(:millisecond)

    assert :ok = SupervisedConnection.store_offset(@stream, "test-store-01", offset)

    assert {:ok, ^offset} = SupervisedConnection.query_offset(@stream, "test-store-01")

    :ok = SupervisedConnection.delete_stream(@stream)
    SupervisedConnection.close()
  end

  @stream "test-store-04"
  test "should query stream metadata" do
    {:ok, _} = SupervisedConnection.start_link(host: "localhost", vhost: "/")
    :ok = SupervisedConnection.connect()

    SupervisedConnection.delete_stream(@stream)
    :ok = SupervisedConnection.create_stream(@stream)

    assert {:ok, _} = SupervisedConnection.query_metadata([@stream])

    :ok = SupervisedConnection.delete_stream(@stream)
    SupervisedConnection.close()
  end

  @stream "test-store-05"
  test "should declare and delete a producer" do
    {:ok, _} = SupervisedConnection.start_link(host: "localhost", vhost: "/")
    :ok = SupervisedConnection.connect()

    SupervisedConnection.delete_stream(@stream)
    :ok = SupervisedConnection.create_stream(@stream)

    # The producerId sequence should always start at 1
    assert {:ok, 1} = SupervisedConnection.declare_producer(@stream, "producer-01")

    assert :ok = SupervisedConnection.delete_producer(1)

    :ok = SupervisedConnection.delete_stream(@stream)
    SupervisedConnection.close()
  end

  @stream "test-store-06"
  @producer "producer-02"
  test "should query producer sequence" do
    {:ok, _} = SupervisedConnection.start_link(host: "localhost", vhost: "/")
    :ok = SupervisedConnection.connect()
    SupervisedConnection.delete_stream(@stream)
    :ok = SupervisedConnection.create_stream(@stream)
    {:ok, _} = SupervisedConnection.declare_producer(@stream, @producer)

    # Should be 0 since the producer was just declared
    assert {:ok, 0} = SupervisedConnection.query_producer_sequence(@stream, @producer)

    :ok = SupervisedConnection.delete_producer(1)
    :ok = SupervisedConnection.delete_stream(@stream)
    SupervisedConnection.close()
  end

  @stream "test-store-07"
  test "should get stream stats" do
    {:ok, _} = SupervisedConnection.start_link(host: "localhost", vhost: "/")
    :ok = SupervisedConnection.connect()
    SupervisedConnection.delete_stream(@stream)
    :ok = SupervisedConnection.create_stream(@stream)
    assert {:ok, _data} = SupervisedConnection.stream_stats(@stream)
    assert {:error, :stream_does_not_exist} = SupervisedConnection.stream_stats("#{@stream}-NON-EXISTENT")
  end

  # I'm not really sure how to test this.
  # @stream "consumer-test-stream-11"
  # test "should buffer user commands before the connection is open" do
  #   {:ok, _conn} = SupervisedConnection.start_link(host: "localhost", vhost: "/")
  #   :ok = SupervisedConnection.connect()

  # end
end

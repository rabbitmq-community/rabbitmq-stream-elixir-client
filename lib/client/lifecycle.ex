defmodule RabbitMQStream.Client.Lifecycle do
  use GenServer
  require Logger
  alias RabbitMQStream.Client
  alias RabbitMQStream.Message

  @moduledoc """
  This module defines the lifecycle of the RabbitMQStream.Client.

  It is responsible for setting up the connections and routing requests to one or more RabbitMQ
  servers within a cluster. It uses the protocol's nodes disovery mechanisms, mainly `query_metadata`
  to resolve the leader of each stream.

  ### Subscription

  A subscribe request is handled by creating a new `RabbitMQStream.Connection` process, being
  responsible for only that subscriber. If the subscriber dies, the connection is also killed.


  How each command is handled by the Client
  - `:connect`: NOOP
  - `:close`: NOOP
  - `:create_stream`: Forwarded to control (Issue: The stream's leader end's up being in the control's node)
  - `:delete_stream`: Forwarded to control
  - `:query_offset`: Forwarded to control
  - `:delete_producer`: Forwarded to broker
  - `:query_metadata`: Forwarded to control
  - `:unsubscribe`: Forwarded to broker
  - `:subscribe`: Spawns a new connection
  - `:credit`: Forwarded to broker
  - `:stream_stats`: Forwarded to control
  - `:partitions`: Forwarded to control
  - `:route`: Forwarded to control
  - `:delete_super_stream`: Forwarded to control
  - `:respond`: Forwarded to broker
  - `:supports?`: Forwarded to control
  - `:query_producer_sequence`: Forwarded to control
  - `:create_super_stream`: Forwarded to control
  - `:declare_producer`: Spawns a new connection
  - `:publish`: Forwarded to broker

  """

  @impl true
  def init(opts) do
    {client_opts, connection_opts} = Keyword.split(opts, [:auto_discovery, :name, :max_retries, :proxied?])

    # We specifically want to ignore the 'lazy' option when setting up a child connection
    connection_opts = Keyword.drop(connection_opts, [:lazy])

    client_opts = Keyword.put(client_opts, :connection_opts, connection_opts)

    conn =
      struct(
        RabbitMQStream.Client,
        Keyword.put(client_opts, :opts, client_opts)
      )

    {:ok, conn, {:continue, :setup}}
  end

  @impl true
  def handle_continue(:setup, conn) do
    connection_opts = Keyword.get(conn.opts, :connection_opts, [])
    # First we start the control connection with the provided options as default
    {:ok, control} = RabbitMQStream.Connection.start_link(connection_opts)

    # Ensure it is connected
    :ok = RabbitMQStream.Connection.connect(control)

    conn =
      conn
      |> Map.put(:control, control)
      |> Map.put(:status, :open)

    {:noreply, conn}
  end

  # 1. Should client's commands other than 'subscribe' be sent to control or its own connection?
  #

  @impl true
  def handle_call({type, opts} = args, _from, %Client{} = conn) when type in [:subscribe, :declare_producer] do
    stream = Keyword.fetch!(opts, :stream_name)
    client_pid = Keyword.fetch!(opts, :pid)

    # We start a new conneciton on every 'subscribe' request.
    if Keyword.has_key?(opts, :subscription_id) || Keyword.has_key?(opts, :producer_id) do
      Logger.error("Manually passing `producer_id` or `subscription_id` to a Client is not allowed")
      {:reply, {:ok, :not_allowed}, conn}
    else
      case new_leader_connection(conn, stream) do
        {:ok, broker_pid} ->
          {id, conn} = Map.get_and_update!(conn, :client_sequence, &{&1, &1 + 1})
          # Listen to each process's lifecycle
          client_ref = Process.monitor(client_pid)
          brooker_ref = Process.monitor(broker_pid)

          opts =
            case type do
              :subscribe ->
                Keyword.put(opts, :subscription_id, id)

              :declare_producer ->
                Keyword.put(opts, :producer_id, id)
            end

          # Forward the request to the new connection
          case GenServer.call(broker_pid, {type, opts}) do
            {:ok, ^id} ->
              # Adds it to the subscriptions list
              monitors =
                conn.monitors
                # And we add both so we can get each other
                |> Map.put(client_ref, {:client, brooker_ref, id})
                |> Map.put(brooker_ref, {:brooker, client_ref, id})

              clients =
                conn.clients
                |> Map.put(id, {client_pid, broker_pid, args})

              {:reply, {:ok, id}, %{conn | monitors: monitors, clients: clients}}

            {:error, error} ->
              {:reply, error, conn}
          end

        reply ->
          {:reply, reply, conn}
      end
    end
  end

  def handle_call({type, opts}, _from, %Client{} = conn)
      when type in [
             :query_offset,
             :store_offset,
             :query_metadata,
             :query_producer_sequence,
             :stream_stats,
             :partitions,
             :route,
             :delete_super_stream,
             :create_stream,
             :delete_stream,
             :create_super_stream
           ] do
    {:reply, GenServer.call(conn.control, {type, opts}), conn}
  end

  def handle_call({:unsubscribe, opts}, _from, %Client{} = conn) do
    broker_pid =
      conn.clients
      |> Map.get(opts[:subscription_id])
      |> then(&elem(&1, 1))

    # TODO: Cleanup
    {:reply, GenServer.call(broker_pid, {:unsubscribe, opts}), conn}
  end

  def handle_call({:delete_producer, opts}, _from, %Client{} = conn) do
    broker_pid =
      conn.clients
      |> Map.get(opts[:producer_id])
      |> then(&elem(&1, 1))

    # TODO: Cleanup
    {:reply, GenServer.call(broker_pid, {:delete_producer, opts}), conn}
  end

  # Noop
  def handle_call({type, _opts}, _from, %Client{} = conn)
      when type in [:connect, :close] do
    Logger.warning("Calling \"#{type}\" on a Client has no effect.")
    {:reply, :ok, conn}
  end

  @impl true
  def handle_cast({:credit, opts}, %Client{} = conn) do
    broker_pid =
      conn.clients
      |> Map.get(opts[:subscription_id])
      |> then(&elem(&1, 1))

    GenServer.cast(broker_pid, {:credit, opts})
    {:noreply, conn}
  end

  def handle_cast({:respond, opts}, %Client{} = conn) do
    # We only accept one type of 'respond' command
    %Message.Request{
      command: :consumer_update,
      data: %Message.Types.ConsumerUpdateRequestData{subscription_id: subscription_id}
    } = Keyword.fetch!(opts, :request)

    broker_pid =
      conn.clients
      |> Map.get(subscription_id)
      |> then(&elem(&1, 1))

    GenServer.cast(broker_pid, {:respond, opts})
    {:noreply, conn}
  end

  # An issue with only forwarding the messages to the broker is that it adds an extra message pass.
  # To workaround this issue we could buffer messages so that it offsets the possible performance hit.
  # Or we could attempt to use ':ets' in some way to prevent work around this, but there doesn't
  # seem to be a way to do this while keeping the exact same interface as a Connection.
  def handle_cast({:publish, opts}, %Client{} = conn) do
    broker_pid =
      conn.clients
      |> Map.get(opts[:producer_id])
      |> then(&elem(&1, 1))

    GenServer.cast(broker_pid, {:publish, opts})
    {:noreply, conn}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %Client{} = conn) do
    case Map.get(conn.clients, ref) do
      {type, other_ref, id} ->
        {client_pid, broker_pid, args} = Map.fetch!(conn.clients, id)

        case type do
          # If the process that has shut down is Client process
          :client ->
            # We should shut down the connection as it is not needed anymore
            :ok = GenServer.stop(broker_pid, :normal, 1000)

          # If it was the broker that shutdown
          :brooker ->
            # We tell the client process that the connection has exited, so it can handle
            # it the way it sees fit. Meaning that a Client might re-run the offset-fetching +
            # subscribing flow, while a producer has to redeclare itself
            case args do
              {:subscribe, opts} ->
                # We forward the 'opts' so that the user's process can use it if needed.
                send(client_pid, {:resubscribe, opts})

              {:declare_producer, opts} ->
                # We forward the 'opts' so that the user's process can use it if needed.
                send(client_pid, {:redeclare_producer, opts})
            end
        end

        # We always do this cleanup as the 'user' process is responsible for any 'init' or 're-init'
        # flow. It also applies to the 'clients' tracker
        monitors = Map.drop(conn.monitors, [ref, other_ref])
        clients = Map.drop(conn.clients, [id])

        {:noreply, %{conn | monitors: monitors, clients: clients}}

      nil ->
        Logger.warning("Received :DOWN for #{ref}, but it was not found in 'clients'")
        {:noreply, conn}
    end
  end

  # Creates a new connection to the leader of the stream
  defp new_leader_connection(%Client{} = conn, stream, _attempts \\ nil) do
    # Query metadata to identify brokers
    case RabbitMQStream.Connection.query_metadata(conn.control, [stream]) do
      {:ok, %{streams: [%{code: :ok, name: ^stream, leader: leader}]} = metadata} ->
        %{host: host, port: port} = Enum.find(metadata.brokers, &(&1.reference == leader))

        # Start a new connection to the leader
        {:ok, broker_conn} =
          RabbitMQStream.Connection.start_link(
            Keyword.merge(
              conn.opts[:connection_opts],
              host: host,
              port: port
            )
          )

        # Ensure the connection is up
        :ok = RabbitMQStream.Connection.connect(broker_conn)

        {:ok, broker_conn}

      _ ->
        {:error, :no_broker_available}
    end
  end
end

defmodule RabbitMQStream.Connection.Lifecycle do
  @moduledoc false

  require Logger

  use GenServer

  alias RabbitMQStream.Message.Request
  alias RabbitMQStream.Connection
  alias RabbitMQStream.Connection.{Handler, Helpers}

  alias RabbitMQStream.Message
  alias RabbitMQStream.Message.{Buffer, Encoder}

  @impl GenServer
  def init(opts) do
    opts =
      opts
      |> Keyword.put_new(:host, "localhost")
      |> Keyword.put_new(:port, 5552)
      |> Keyword.put_new(:vhost, "/")
      |> Keyword.put_new(:username, "guest")
      |> Keyword.put_new(:password, "guest")
      |> Keyword.put_new(:frame_max, 1_048_576)
      |> Keyword.put_new(:heartbeat, 60)
      |> Keyword.put_new(:transport, :tcp)

    {transport, opts} = Keyword.pop(opts, :transport, :tcp)

    transport =
      case transport do
        :tcp -> RabbitMQStream.Connection.Transport.TCP
        :ssl -> RabbitMQStream.Connection.Transport.SSL
        transport -> transport
      end

    conn = %RabbitMQStream.Connection{options: opts, transport: transport}

    if opts[:lazy] == true do
      {:ok, conn}
    else
      {:ok, conn, {:continue, {:connect}}}
    end
  end

  @impl GenServer
  def handle_call({:connect}, from, %Connection{state: :closed} = conn) do
    Logger.debug("Connecting to server: #{conn.options[:host]}:#{conn.options[:port]}")

    with {:ok, conn} <- connect(conn) do
      Logger.debug("Connection stablished. Initiating properties exchange.")

      conn =
        %{conn | connect_requests: [from | conn.connect_requests]}
        |> Helpers.push_internal(:request, :peer_properties)
        |> flush_buffer(:internal)

      {:noreply, conn}
    else
      err ->
        Logger.error("Failed to connect to #{conn.options[:host]}:#{conn.options[:port]}")
        {:reply, {:error, err}, conn}
    end
  end

  def handle_call({:connect}, _from, %Connection{state: :open} = conn) do
    {:reply, :ok, conn}
  end

  def handle_call({:connect}, from, %Connection{} = conn) do
    {:noreply, %{conn | connect_requests: [from | conn.connect_requests]}}
  end

  # Replies with `:ok` if the connection is already closed. Not sure if this behavior is the best.
  def handle_call({:close, _reason, _code}, _from, %Connection{state: :closed} = conn) do
    {:reply, :ok, conn}
  end

  def handle_call(action, from, %Connection{state: state} = conn) when state != :open do
    {:noreply, %{conn | request_buffer: :queue.in({:call, {action, from}}, conn.request_buffer)}}
  end

  def handle_call({:close, reason, code}, from, %Connection{} = conn) do
    Logger.debug("Connection close requested by client: #{reason} #{code}")

    conn =
      conn
      |> Helpers.push_tracker(:close, from)
      |> send_request(:close, reason: reason, code: code)

    {:noreply, conn}
  end

  def handle_call({:subscribe, opts}, from, %Connection{} = conn) do
    {opts, conn} =
      if Keyword.has_key?(opts, :subscription_id) do
        {opts, conn}
      else
        {id, conn} = Map.get_and_update!(conn, :subscriber_sequence, &{&1, &1 + 1})

        opts = Keyword.put(opts, :subscription_id, id)

        {opts, conn}
      end

    conn =
      conn
      |> Helpers.push_tracker(:subscribe, from, {opts[:subscription_id], opts[:pid]})
      |> send_request(:subscribe, opts)

    {:noreply, conn}
  end

  def handle_call({:unsubscribe, opts}, from, %Connection{} = conn) do
    conn =
      conn
      |> Helpers.push_tracker(:unsubscribe, from, opts[:subscription_id])
      |> send_request(:unsubscribe, opts)

    {:noreply, conn}
  end

  def handle_call({:supports?, opts}, _from, %Connection{} = conn) do
    command = Keyword.get(opts, :command)
    version = Keyword.get(opts, :version)

    flag =
      case conn.commands do
        %{^command => %{max: max}} when max <= version ->
          true

        _ ->
          false
      end

    {:reply, flag, conn}
  end

  def handle_call({command, opts}, from, %Connection{} = conn)
      when command in [
             :query_offset,
             :delete_producer,
             :query_metadata,
             :query_producer_sequence,
             :delete_stream,
             :create_stream,
             :stream_stats
           ] do
    conn =
      conn
      |> Helpers.push_tracker(command, from)
      |> send_request(command, opts)

    {:noreply, conn}
  end

  def handle_call({command, opts}, from, %Connection{} = conn)
      when command in [:route, :partitions, :create_super_stream, :delete_super_stream] and
             is_map_key(conn.commands, command) do
    conn =
      conn
      |> Helpers.push_tracker(command, from)
      |> send_request(command, opts)

    {:noreply, conn}
  end

  def handle_call({command, _opts}, _from, %Connection{peer_properties: %{"version" => version}} = conn)
      when command in [:route, :partitions, :create_super_stream, :delete_super_stream] do
    Logger.error("Command #{command} is not supported by the server. Its current informed version is '#{version}'.")

    {:reply, {:error, :unsupported}, conn}
  end

  def handle_call({:declare_producer, opts}, from, %Connection{} = conn) do
    {opts, conn} =
      if Keyword.has_key?(opts, :producer_id) do
        {opts, conn}
      else
        {id, conn} = Map.get_and_update!(conn, :producer_sequence, &{&1, &1 + 1})

        opts = Keyword.put(opts, :producer_id, id)

        {opts, conn}
      end

    conn =
      conn
      |> Helpers.push_tracker(:declare_producer, from, opts[:producer_id])
      |> send_request(:declare_producer, opts)

    {:noreply, conn}
  end

  @impl GenServer
  # User facing events should be handled only when the connection is open.
  def handle_cast(action, %Connection{state: state} = conn) when state != :open do
    {:noreply, %{conn | request_buffer: :queue.in({:cast, action}, conn.request_buffer)}}
  end

  def handle_cast({:store_offset, opts}, %Connection{} = conn) do
    conn =
      conn
      |> send_request(:store_offset, opts)

    {:noreply, conn}
  end

  def handle_cast({:publish, opts}, %Connection{} = conn) do
    conn =
      case {opts[:message], conn} do
        {{_, _, filter_value}, conn} when is_binary(filter_value) and conn.commands.publish.max >= 2 ->
          Logger.error("Publishing a message with a `filter_value` is only supported by RabbitMQ on versions >= 3.13")

          conn

        _ ->
          conn
          |> send_request(:publish, opts ++ [correlation_sum: 0])
      end

    {:noreply, conn}
  end

  def handle_cast({:credit, opts}, %Connection{} = conn) do
    conn =
      conn
      |> send_request(:credit, opts)

    {:noreply, conn}
  end

  def handle_cast({:respond, opts}, %Connection{} = conn) do
    %Request{} = request = Keyword.fetch!(opts, :request)
    opts = Keyword.fetch!(opts, :opts)

    conn =
      conn
      |> send_response(request.command, [correlation_id: request.correlation_id] ++ opts)

    {:noreply, conn}
  end

  @impl GenServer
  def handle_info({key, _socket, data}, conn) when key in [:ssl, :tcp] do
    {commands, frames_buffer} =
      data
      |> Buffer.incoming_data(conn.frames_buffer)
      |> Buffer.all_commands()

    conn = %{conn | frames_buffer: frames_buffer}

    # A single frame can have multiple commands, and each might have multiple responses.
    # So we first handle each received command, and only then we 'flush', or send, each
    # command to the socket. This also would allow us to better test the 'handler' logic.
    commands
    |> Enum.reduce(conn, &Handler.handle_message(&2, &1))
    |> flush_buffer(:internal)
    |> flush_buffer(:user)
    |> handle_closing()
  end

  def handle_info({key, _socket}, conn) when key in [:tcp_closed, :ssl_closed] do
    if conn.state == :connecting do
      Logger.warning(
        "The connection was closed by the host, after the socket was already open, while running the authentication sequence. This could be caused by the server not having Stream Plugin active"
      )
    end

    %{conn | close_reason: key}
    |> handle_closing()
  end

  def handle_info({key, _socket, reason}, conn) when key in [:tcp_error, :ssl_error] do
    %{conn | close_reason: reason}
    |> handle_closing()
  end

  def handle_info({:heartbeat}, conn) do
    Process.send_after(self(), {:heartbeat}, conn.options[:heartbeat] * 1000)

    conn =
      conn
      |> Helpers.push_internal(:request, :heartbeat, correlation_sum: 0)
      |> flush_buffer(:internal)

    {:noreply, conn}
  end

  def handle_info(:flush_request_buffer, %Connection{state: :closed} = conn) do
    Logger.warning("Connection is closed. Ignoring flush buffer request.")
    {:noreply, conn}
  end

  # I'm not really sure how to test this behavior at the moment.
  def handle_info(:flush_request_buffer, %Connection{} = conn) do
    # There is probably a better way to reprocess the buffer, but I'm not sure how to do at the moment.
    conn =
      :queue.fold(
        fn
          {:call, {action, from}}, conn ->
            {:noreply, conn} = handle_call(action, from, conn)
            conn

          {:cast, action}, conn ->
            {:noreply, conn} = handle_cast(action, conn)
            conn
        end,
        conn,
        conn.request_buffer
      )

    {:noreply, %{conn | request_buffer: :queue.new()}}
  end

  @impl GenServer
  def handle_continue({:connect}, %Connection{state: :closed} = conn) do
    Logger.debug("Connecting to server: #{conn.options[:host]}:#{conn.options[:port]}")

    with {:ok, conn} <- connect(conn) do
      Logger.debug("Connection stablished. Initiating properties exchange.")

      conn =
        conn
        |> Helpers.push_internal(:request, :peer_properties)
        |> flush_buffer(:internal)

      {:noreply, conn}
    else
      _ ->
        Logger.error("Failed to connect to #{conn.options[:host]}:#{conn.options[:port]}")
        {:noreply, conn}
    end
  end

  defp connect(%Connection{} = conn) do
    with {:ok, socket} <- conn.transport.connect(conn.options) do
      conn =
        conn
        |> Map.put(:socket, socket)
        |> RabbitMQStream.Connection.Handler.transition(:connecting)

      {:ok, conn}
    end
  end

  defp handle_closing(%Connection{state: :closing} = conn) do
    for request <- conn.connect_requests do
      GenServer.reply(request, {:error, :closed})
    end

    if is_port(conn.socket) do
      :ok = conn.transport.close(conn.socket)
    end

    for {client, _data} <- Map.values(conn.request_tracker) do
      GenServer.reply(client, {:error, {:closed, conn.close_reason}})
    end

    conn =
      %{conn | request_tracker: %{}, connect_requests: [], socket: nil, close_reason: nil}
      |> RabbitMQStream.Connection.Handler.transition(:closed)

    {:noreply, conn, :hibernate}
  end

  defp handle_closing(conn), do: {:noreply, conn}

  defp send_request(%Connection{} = conn, command, opts) do
    conn
    |> Helpers.push_user(:request, command, opts)
    |> flush_buffer(:user)
  end

  defp send_response(%Connection{} = conn, command, opts) do
    conn
    |> Helpers.push_user(:response, command, opts)
    |> flush_buffer(:user)
  end

  defp flush_buffer(%Connection{} = conn, :internal) do
    conn =
      :queue.fold(
        fn
          command, conn ->
            send_command(conn, command)
        end,
        conn,
        conn.internal_buffer
      )

    %{conn | internal_buffer: :queue.new()}
  end

  defp flush_buffer(%Connection{state: :open} = conn, :user) do
    conn =
      :queue.fold(
        fn
          command, conn ->
            send_command(conn, command)
        end,
        conn,
        conn.user_buffer
      )

    %{conn | user_buffer: :queue.new()}
  end

  defp flush_buffer(%Connection{} = conn, :user) do
    conn
  end

  defp send_command(%Connection{} = conn, {:request, command, opts}) do
    {correlation_sum, opts} = Keyword.pop(opts, :correlation_sum, 1)

    frame =
      conn
      |> Message.new_request(command, opts)
      |> Encoder.encode()

    :ok = conn.transport.send(conn.socket, frame)

    %{
      conn
      | correlation_sequence: conn.correlation_sequence + correlation_sum
    }
  end

  defp send_command(%Connection{} = conn, {:response, command, opts}) do
    frame =
      conn
      |> Message.new_response(command, opts)
      |> Encoder.encode()

    :ok = conn.transport.send(conn.socket, frame)

    conn
  end
end

defmodule RabbitMQStream.Connection.Handler do
  @moduledoc false

  require Logger
  alias RabbitMQStream.Message.{Request, Response}
  alias RabbitMQStream.Connection
  alias RabbitMQStream.Connection.Helpers

  def handle_message(%Connection{} = conn, %Request{command: :close} = request) do
    Logger.debug("Connection close requested by server: #{request.data.code} #{request.data.reason}")
    Logger.debug("Connection closed")

    conn
    |> Map.put(:close_reason, request.data.reason)
    |> Helpers.push_internal(:response, :close, correlation_id: request.correlation_id, code: :ok)
    |> transition(:closing)
  end

  def handle_message(%Connection{} = conn, %Request{command: :tune} = request) do
    Logger.debug("Tunning complete. Starting heartbeat timer.")

    Process.send_after(self(), {:heartbeat}, conn.options[:heartbeat] * 1000)

    options = Keyword.merge(conn.options, frame_max: request.data.frame_max, heartbeat: request.data.heartbeat)

    conn
    |> Map.put(:options, options)
    |> Helpers.push_internal(:response, :tune, correlation_id: 0)
    |> Helpers.push_internal(:request, :open)
    |> transition(:opening)
  end

  def handle_message(%Connection{} = conn, %Request{command: :heartbeat}) do
    conn
  end

  def handle_message(%Connection{} = conn, %Request{command: :metadata_update} = request) do
    conn
    |> Helpers.push_internal(:request, :query_metadata, streams: [request.data.stream_name])
  end

  def handle_message(%Connection{} = conn, %Request{command: :deliver} = response) do
    pid = Map.get(conn.subscriptions, response.data.subscription_id)

    if pid != nil do
      send(pid, {:deliver, response.data})
    end

    conn
  end

  def handle_message(%Connection{} = conn, %Request{command: command})
      when command in [:publish_confirm, :publish_error] do
    conn
  end

  def handle_message(%Connection{} = conn, %Response{command: :close} = response) do
    Logger.debug("Connection closed: #{conn.options[:host]}:#{conn.options[:port]}")

    {{pid, _data}, conn} = Helpers.pop_tracker(conn, :close, response.correlation_id)

    GenServer.reply(pid, :ok)

    transition(conn, :closed)
  end

  def handle_message(%Connection{} = conn, %Response{code: code})
      when code in [
             :sasl_mechanism_not_supported,
             :authentication_failure,
             :sasl_error,
             :sasl_challenge,
             :sasl_authentication_failure_loopback,
             :virtual_host_access_failure
           ] do
    Logger.error("Failed to connect to #{conn.options[:host]}:#{conn.options[:port]}. Reason: #{code}")

    for request <- conn.connect_requests do
      GenServer.reply(request, {:error, code})
    end

    conn
    |> Map.put(:close_reason, code)
    |> transition(:closing)
  end

  def handle_message(%Connection{} = conn, %Response{command: :credit, code: code})
      when code not in [:ok, nil] do
    Logger.error("Failed to credit subscription. Reason: #{code}")

    conn
  end

  def handle_message(%Connection{} = conn, %Response{command: command, code: code} = response)
      when command in [
             :create_stream,
             :delete_stream,
             :query_offset,
             :declare_producer,
             :delete_producer,
             :subscribe,
             :unsubscribe,
             :stream_stats,
             :create_super_stream,
             :delete_super_stream,
             :route,
             :partitions
           ] and
             code not in [:ok, nil] do
    {{pid, _data}, conn} = Helpers.pop_tracker(conn, command, response.correlation_id)

    if pid != nil do
      GenServer.reply(pid, {:error, code})
    end

    conn
  end

  def handle_message(%Connection{state: :closed} = conn, _) do
    Logger.error("Message received on a closed connection")

    conn
  end

  def handle_message(%Connection{} = conn, %Response{command: :peer_properties} = response) do
    Logger.debug("Peer Properties exchange successful. Initiating SASL handshake.")

    # We need to extract the base version from the version string so we can compare
    # make decisions based on the version of the server.
    base_version = Version.parse!(response.data.peer_properties["version"])

    peer_properties = Map.put(response.data.peer_properties, "base-version", base_version)

    %{conn | peer_properties: peer_properties}
    |> Helpers.push_internal(:request, :sasl_handshake)
  end

  def handle_message(%Connection{} = conn, %Response{command: :sasl_handshake} = response) do
    Logger.debug("SASL handshake successful. Initiating authentication.")

    %{conn | mechanisms: response.data.mechanisms}
    |> Helpers.push_internal(:request, :sasl_authenticate)
  end

  def handle_message(%Connection{} = conn, %Response{command: :sasl_authenticate, data: %{sasl_opaque_data: ""}}) do
    Logger.debug("Authentication successful. Initiating connection tuning.")

    conn
  end

  def handle_message(%Connection{} = conn, %Response{command: :sasl_authenticate}) do
    Logger.debug("Authentication successful. Skipping connection tuning.")
    Logger.debug("Opening connection to vhost: \"#{conn.options[:vhost]}\"")

    conn
    |> Helpers.push_internal(:request, :open)
    |> transition(:opening)
  end

  def handle_message(%Connection{} = conn, %Response{command: :tune} = response) do
    Logger.debug("Tunning data received. Starting heartbeat timer.")
    Logger.debug("Opening connection to vhost: \"#{conn.options[:vhost]}\"")

    options = Keyword.merge(conn.options, frame_max: response.data.frame_max, heartbeat: response.data.heartbeat)

    %{conn | options: options}
    |> Helpers.push_internal(:request, :open)
    |> transition(:opening)
  end

  # If the server has a version lower than 3.13, this is the 'terminating' response.
  def handle_message(
        %Connection{peer_properties: %{"base-version" => version}} = conn,
        %Response{command: :open} = response
      )
      when [version.major, version.minor] < [3, 13] do
    Logger.debug("Successfully opened connection with vhost: \"#{conn.options[:vhost]}\"")

    for request <- conn.connect_requests do
      GenServer.reply(request, :ok)
    end

    send(self(), :flush_request_buffer)

    conn
    |> Map.put(:connect_requests, [])
    |> Map.put(:connection_properties, response.data.connection_properties)
    |> transition(:open)
  end

  def handle_message(
        %Connection{peer_properties: %{"base-version" => version}} = conn,
        %Response{command: :open} = response
      )
      when [version.major, version.minor] >= [3, 13] do
    Logger.debug(
      "Successfully opened connection with vhost: \"#{conn.options[:vhost]}\". Initiating command version exchange."
    )

    %{conn | connection_properties: response.data.connection_properties}
    |> Helpers.push_internal(:request, :exchange_command_versions)
  end

  def handle_message(%Connection{} = conn, %Response{command: :query_metadata} = response) do
    {{pid, _data}, conn} = Helpers.pop_tracker(conn, :query_metadata, response.correlation_id)

    if pid != nil do
      GenServer.reply(pid, {:ok, response.data})
    end

    conn
  end

  def handle_message(%Connection{} = conn, %Response{command: :query_offset} = response) do
    {{pid, _data}, conn} = Helpers.pop_tracker(conn, :query_offset, response.correlation_id)

    if pid != nil do
      GenServer.reply(pid, {:ok, response.data.offset})
    end

    conn
  end

  def handle_message(%Connection{} = conn, %Response{command: :declare_producer} = response) do
    {{pid, id}, conn} = Helpers.pop_tracker(conn, :declare_producer, response.correlation_id)

    if pid != nil do
      GenServer.reply(pid, {:ok, id})
    end

    conn
  end

  def handle_message(%Connection{} = conn, %Response{command: :query_producer_sequence} = response) do
    {{pid, _data}, conn} = Helpers.pop_tracker(conn, :query_producer_sequence, response.correlation_id)

    if pid != nil do
      GenServer.reply(pid, {:ok, response.data.sequence})
    end

    conn
  end

  def handle_message(%Connection{} = conn, %Response{command: :subscribe} = response) do
    {{pid, data}, conn} = Helpers.pop_tracker(conn, :subscribe, response.correlation_id)

    {subscription_id, consumer} = data

    if pid != nil do
      GenServer.reply(pid, {:ok, subscription_id})
    end

    %{conn | subscriptions: Map.put(conn.subscriptions, subscription_id, consumer)}
  end

  def handle_message(%Connection{} = conn, %Response{command: :unsubscribe} = response) do
    {{pid, subscription_id}, conn} = Helpers.pop_tracker(conn, :unsubscribe, response.correlation_id)

    if pid != nil do
      GenServer.reply(pid, :ok)
    end

    %{conn | subscriptions: Map.drop(conn.subscriptions, [subscription_id])}
  end

  # If the server has a version 3.12 or higher, this is the 'terminating' response.
  def handle_message(%Connection{} = conn, %Response{command: :exchange_command_versions} = response) do
    {{pid, _}, conn} = Helpers.pop_tracker(conn, :exchange_command_versions, response.correlation_id)

    if pid != nil do
      GenServer.reply(pid, {:ok, response.data})
    end

    commands =
      Map.new(response.data.commands, fn command ->
        {command.key, %{min: command.min_version, max: command.max_version}}
      end)

    for request <- conn.connect_requests do
      GenServer.reply(request, :ok)
    end

    send(self(), :flush_request_buffer)

    conn
    |> Map.merge(%{connect_requests: [], commands: commands})
    |> transition(:open)
  end

  def handle_message(%Connection{} = conn, %Response{command: command, data: data} = response)
      when command in [:route, :partitions, :stream_stats] do
    {{pid, _data}, conn} = Helpers.pop_tracker(conn, command, response.correlation_id)

    if pid != nil do
      GenServer.reply(pid, {:ok, data})
    end

    conn
  end

  def handle_message(%Connection{} = conn, %Response{command: command} = response)
      when command in [:create_stream, :delete_stream, :delete_producer, :create_super_stream, :delete_super_stream] do
    {{pid, _data}, conn} = Helpers.pop_tracker(conn, command, response.correlation_id)

    if pid != nil do
      GenServer.reply(pid, :ok)
    end

    conn
  end

  # We forward the request because the consumer is the one responsible for
  # deciding how to respond to the request.
  def handle_message(%Connection{} = conn, %Request{command: :consumer_update} = request) do
    subscription_pid = Map.get(conn.subscriptions, request.data.subscription_id)

    if subscription_pid != nil do
      send(subscription_pid, {:command, request})
      conn
    else
      conn
      |> Helpers.push_internal(:response, :consumer_update,
        correlation_id: request.correlation_id,
        code: :internal_error
      )
    end
  end

  @doc """
  Transitions the lifecycle of the connection to the given state, while notifying all monitors.

  Always call this function after all the state manipulation of the transition is done.
  """
  def transition(%Connection{} = conn, to) when is_atom(to) do
    # TODO: Telemetry events
    %{conn | state: to}
  end
end

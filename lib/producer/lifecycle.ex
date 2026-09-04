defmodule RabbitMQStream.Producer.LifeCycle do
  @moduledoc false
  use GenServer
  require Logger

  alias RabbitMQStream.Connection.Router

  # Callbacks
  @impl GenServer
  def init(opts \\ []) do
    reference_name = Keyword.get(opts, :reference_name, Atom.to_string(opts[:producer_module]))
    connection = Keyword.get(opts, :connection) || raise(":connection is required")
    stream_name = Keyword.get(opts, :stream_name) || raise(":stream_name is required")

    state = %RabbitMQStream.Producer{
      id: nil,
      sequence: nil,
      stream_name: stream_name,
      connection: connection,
      seed_connection: connection,
      reference_name: reference_name,
      producer_module: opts[:producer_module]
    }

    {:ok, state, {:continue, opts}}
  end

  @impl GenServer
  def handle_continue(opts, state) do
    # `before_start/2` runs first, unchanged from before this feature existed — it's
    # commonly used to create the stream itself, so leader resolution (which needs the
    # stream to already exist) is deliberately done afterwards, not before.
    state =
      if state.producer_module != nil and function_exported?(state.producer_module, :before_start, 2) do
        apply(state.producer_module, :before_start, [opts, state])
      else
        state
      end

    state = resolve_connection(state)
    Router.monitor(state.seed_connection, state.connection)

    with {:ok, id} <-
           RabbitMQStream.Connection.declare_producer(state.connection, state.stream_name, state.reference_name),
         {:ok, sequence} <-
           RabbitMQStream.Connection.query_producer_sequence(state.connection, state.stream_name, state.reference_name) do
      {:noreply, %{state | id: id, sequence: sequence + 1}}
    else
      err ->
        {:stop, err, state}
    end
  end

  @impl GenServer
  def handle_cast({:publish, {message, filter_value}}, %RabbitMQStream.Producer{} = state) when is_binary(message) do
    :ok = RabbitMQStream.Connection.publish(state.connection, state.id, state.sequence, message, filter_value)

    {:noreply, %{state | sequence: state.sequence + 1}}
  end

  # A routed (non-seed) connection dying means the publisher `id` declared on it is
  # gone too — it was only ever valid for that specific connection's session. There's
  # nothing to reconcile in place (that's reconnection, a separate concern), so stop
  # cleanly and let the caller's own supervisor restart this producer from scratch,
  # which re-resolves routing and re-declares the producer against a live connection.
  @impl GenServer
  def handle_info({:DOWN, _ref, :process, pid, reason}, %{connection: pid} = state) do
    Logger.error(
      "#{state.producer_module}: Connection #{inspect(pid)} for stream #{state.stream_name} went down (#{inspect(reason)}). Stopping so the supervisor can restart it."
    )

    {:stop, {:connection_down, reason}, state}
  end

  @impl GenServer
  def terminate({:connection_down, _reason}, _state), do: :ok

  def terminate(_reason, %{id: nil}), do: :ok

  def terminate(_reason, state) do
    RabbitMQStream.Connection.delete_producer(state.connection, state.id)
    :ok
  end

  # Resolves the stream's leader and swaps `state.connection` to it. Any resolution
  # failure (stream not found yet, metadata unreachable, etc.) is non-fatal: it just
  # keeps using whatever connection is already set, so a producer that can't resolve
  # leader routing behaves exactly as it did before this feature existed.
  defp resolve_connection(state) do
    case Router.producer_connection(state.seed_connection, state.stream_name) do
      {:ok, connection} ->
        %{state | connection: connection}

      {:error, reason} ->
        Logger.warning(
          "#{state.producer_module}: Failed to resolve leader for stream #{state.stream_name}, falling back to the seed connection. Reason: #{inspect(reason)}"
        )

        state
    end
  end
end

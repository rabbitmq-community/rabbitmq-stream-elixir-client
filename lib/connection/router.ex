defmodule RabbitMQStream.Connection.Router do
  @moduledoc false

  # Single call site `Producer.LifeCycle`/`Consumer.LifeCycle` use to resolve which
  # connection to actually use for a given stream: producers always target the leader;
  # consumers prefer a random replica, falling back to the leader when there are none
  # (matching the verified default behavior of the Java and .NET clients).

  require Logger

  alias RabbitMQStream.Connection
  alias RabbitMQStream.Connection.{Pool, MetadataResolver}

  @spec producer_connection(seed :: GenServer.server(), stream_name :: String.t()) ::
          {:ok, GenServer.server()} | {:error, term()}
  def producer_connection(seed, stream_name) do
    with {:ok, resolved} <- resolve(seed, stream_name) do
      route(seed, resolved.leader)
    end
  end

  @spec consumer_connection(seed :: GenServer.server(), stream_name :: String.t()) ::
          {:ok, GenServer.server()} | {:error, term()}
  def consumer_connection(seed, stream_name) do
    with {:ok, resolved} <- resolve(seed, stream_name) do
      route(seed, pick_replica(resolved))
    end
  end

  defp resolve(seed, stream_name) do
    with {:ok, metadata} <- Connection.query_metadata(seed, [stream_name]) do
      MetadataResolver.resolve(metadata, stream_name)
    end
  end

  defp pick_replica(%{replicas: []} = resolved), do: resolved.leader
  defp pick_replica(%{replicas: replicas}), do: Enum.random(replicas)

  # Shared by `Producer.LifeCycle`/`Consumer.LifeCycle`: resolves the effective
  # connection to use for `stream_name` and returns it directly (never an error —
  # a resolution failure just falls back to whatever `connection` already is,
  # logging why). `before_start/2` runs before this and may have deliberately set
  # `connection` to something other than `seed` for its own routing logic; treat
  # that as an explicit opt-out rather than silently overriding it right after
  # startup, which is what happened before this existed.
  @spec resolve_connection(
          kind :: :producer | :consumer,
          module_tag :: module(),
          seed :: GenServer.server(),
          connection :: GenServer.server(),
          stream_name :: String.t()
        ) :: GenServer.server()
  def resolve_connection(kind, module_tag, seed, connection, stream_name) when connection == seed do
    case do_resolve(kind, seed, stream_name) do
      {:ok, resolved_connection} ->
        resolved_connection

      {:error, reason} ->
        Logger.warning(
          "#{module_tag}: Failed to resolve #{resolve_label(kind)} for stream #{stream_name}, falling back to the seed connection. Reason: #{inspect(reason)}"
        )

        connection
    end
  end

  def resolve_connection(_kind, _module_tag, _seed, connection, _stream_name), do: connection

  defp do_resolve(:producer, seed, stream_name), do: producer_connection(seed, stream_name)
  defp do_resolve(:consumer, seed, stream_name), do: consumer_connection(seed, stream_name)

  defp resolve_label(:producer), do: "leader"
  defp resolve_label(:consumer), do: "leader/replica"

  # A routed connection comes from the shared Pool, not from the caller's own
  # supervision tree, so nothing else notifies a Producer/Consumer if it dies (a
  # pooled `Connection` restarting under `DynamicSupervisor` gets a fresh pid,
  # and the old one silently no-ops forever against `GenServer.cast`). Monitor it
  # so the caller can detect that and stop cleanly instead. Not needed for the
  # seed connection: that one lives in the caller's own supervision tree already.
  @spec monitor(seed :: GenServer.server(), connection :: GenServer.server()) :: reference() | nil
  def monitor(seed, connection) when connection != seed, do: Process.monitor(connection)
  def monitor(_seed, _connection), do: nil

  # Avoids opening a redundant pooled connection when the seed itself already is the
  # resolved broker (the common single-node case).
  defp route(seed, %{host: host, port: port}) do
    base_options = Connection.get_options(seed)

    if base_options[:host] == host and base_options[:port] == port do
      {:ok, seed}
    else
      Pool.get_or_start_connection(base_options, host, port)
    end
  end
end

defmodule RabbitMQStream.Connection.Router do
  @moduledoc false

  # Single call site `Producer.LifeCycle`/`Consumer.LifeCycle` use to resolve which
  # connection to actually use for a given stream: producers always target the leader;
  # consumers prefer a random replica, falling back to the leader when there are none
  # (matching the verified default behavior of the Java and .NET clients).

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

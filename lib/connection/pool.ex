defmodule RabbitMQStream.Connection.Pool do
  @moduledoc false

  # Per-broker-node connection registry. Lazily dials and reuses a `RabbitMQStream.Connection`
  # for a given host/port, keyed so that different vhosts/credentials/transports never share a
  # socket. The registry/supervisor backing this are started by `RabbitMQStream.Application`,
  # not by this module, so a crash in an unrelated caller can't take the shared pool down with it.
  #
  # Note: does not yet enforce a per-node connection capacity (Java/`.NET` cap producers/consumers
  # per connection at 256) — every distinct key gets exactly one connection. Left as a known,
  # documented gap rather than built speculatively here.

  @registry RabbitMQStream.Connection.Pool.Registry
  @dynamic_supervisor RabbitMQStream.Connection.Pool.DynamicSupervisor

  @type key :: {
          host :: String.t(),
          port :: non_neg_integer(),
          vhost :: String.t(),
          username :: String.t(),
          transport :: atom() | module()
        }

  @spec get_or_start_connection(base_options :: Keyword.t(), host :: String.t(), port :: non_neg_integer()) ::
          {:ok, GenServer.server()} | {:error, term()}
  def get_or_start_connection(base_options, host, port) do
    key = build_key(base_options, host, port)

    case Registry.lookup(@registry, key) do
      [{pid, _}] -> connect(pid)
      [] -> start_and_connect(base_options, host, port, key)
    end
  end

  defp start_and_connect(base_options, host, port, key) do
    opts =
      base_options
      |> Keyword.merge(host: host, port: port)
      |> Keyword.put(:name, {:via, Registry, {@registry, key}})

    case DynamicSupervisor.start_child(@dynamic_supervisor, {RabbitMQStream.Connection, opts}) do
      {:ok, pid} -> connect(pid)
      {:error, {:already_started, pid}} -> connect(pid)
      error -> error
    end
  end

  defp connect(pid) do
    case RabbitMQStream.Connection.connect(pid) do
      :ok -> {:ok, pid}
      error -> error
    end
  end

  defp build_key(base_options, host, port) do
    {
      host,
      port,
      Keyword.get(base_options, :vhost, "/"),
      Keyword.get(base_options, :username, "guest"),
      Keyword.get(base_options, :transport, :tcp)
    }
  end
end

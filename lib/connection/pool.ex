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
    resolve(base_options, host, port, build_key(base_options, host, port), 10)
  end

  # A pid found via `Registry.lookup/2` may have just died with its registration not
  # yet cleaned up — Registry's own monitor processes that asynchronously, on its own
  # schedule, independent of any other party's monitor on the same pid. `connect/1`
  # turns that into `{:error, :noproc}` instead of crashing the caller, and this retries
  # against a freshly-registered connection. Bounded, since an actually-stuck Registry
  # would otherwise recurse forever; in practice this resolves within a retry or two.
  defp resolve(_base_options, _host, _port, _key, 0), do: {:error, :noproc}

  defp resolve(base_options, host, port, key, attempts_left) do
    case Registry.lookup(@registry, key) do
      [{pid, _}] ->
        case connect(pid) do
          {:error, :noproc} -> retry(base_options, host, port, key, attempts_left)
          result -> result
        end

      [] ->
        start_and_connect(base_options, host, port, key, attempts_left)
    end
  end

  defp start_and_connect(base_options, host, port, key, attempts_left) do
    opts =
      base_options
      |> Keyword.merge(host: host, port: port)
      |> Keyword.put(:name, {:via, Registry, {@registry, key}})

    case DynamicSupervisor.start_child(@dynamic_supervisor, {RabbitMQStream.Connection, opts}) do
      {:ok, pid} ->
        connect(pid)

      {:error, {:already_started, pid}} ->
        case connect(pid) do
          {:error, :noproc} -> retry(base_options, host, port, key, attempts_left)
          result -> result
        end

      error ->
        error
    end
  end

  # A dead registration doesn't necessarily clear before the very next lookup: Registry
  # removes it via its own monitor process, scheduled independently of (and not
  # ordered relative to) whatever just observed the death. A bare recursive retry can
  # burn through the whole attempt budget faster than that monitor gets scheduled to
  # run at all, so yield the scheduler a moment first.
  defp retry(base_options, host, port, key, attempts_left) do
    Process.sleep(1)
    resolve(base_options, host, port, key, attempts_left - 1)
  end

  defp connect(pid) do
    RabbitMQStream.Connection.connect(pid)
    |> case do
      :ok -> {:ok, pid}
      error -> error
    end
  catch
    :exit, _ -> {:error, :noproc}
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

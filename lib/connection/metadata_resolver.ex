defmodule RabbitMQStream.Connection.MetadataResolver do
  @moduledoc false

  alias RabbitMQStream.Message.Types.QueryMetadataResponseData
  alias RabbitMQStream.Message.Types.QueryMetadataResponseData.{StreamData, BrokerData}

  @type resolved :: %{leader: BrokerData.t(), replicas: [BrokerData.t()]}

  @doc """
  Resolves a stream's leader/replicas, decoded from `query_metadata/2` as broker
  reference integers, against the response's `brokers` list, into their actual
  dialable `host`/`port`.

  Unresolvable replica references (e.g. a stale/rebalancing replica) are dropped
  from the result rather than failing the whole lookup.
  """
  @spec resolve(QueryMetadataResponseData.t(), String.t()) ::
          {:ok, resolved()} | {:error, :stream_not_found | :leader_not_found}
  def resolve(%QueryMetadataResponseData{streams: streams, brokers: brokers}, stream_name) do
    case Enum.find(streams, &(&1.name == stream_name)) do
      nil ->
        {:error, :stream_not_found}

      %StreamData{code: code} when code != :ok ->
        {:error, :stream_not_found}

      %StreamData{} = stream ->
        broker_index = Map.new(brokers, &{&1.reference, &1})

        case Map.fetch(broker_index, stream.leader) do
          {:ok, leader} ->
            replicas =
              stream.replicas
              |> Enum.map(&Map.get(broker_index, &1))
              |> Enum.reject(&is_nil/1)

            {:ok, %{leader: leader, replicas: replicas}}

          :error ->
            {:error, :leader_not_found}
        end
    end
  end
end

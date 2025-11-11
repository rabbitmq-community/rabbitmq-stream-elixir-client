defmodule RabbitMQStream.Connection.Behavior do
  @moduledoc """
  Defines the interface a Connection Module that interacts with the Streams Protocol TCP/TLS API.
  """
  alias RabbitMQStream.Message.Types.{PartitionsQueryResponseData, StreamStatsResponseData, QueryMetadataResponseData}

  @callback connect(GenServer.server()) :: :ok | {:error, reason :: atom()}

  @callback close(GenServer.server(), reason :: String.t(), code :: integer()) ::
              :ok | {:error, reason :: atom()}

  @callback create_stream(GenServer.server(), stream_name :: String.t(), arguments :: keyword(String.t()) | nil) ::
              :ok | {:error, reason :: atom()}

  @callback delete_stream(GenServer.server(), stream_name :: String.t()) :: :ok | {:error, reason :: atom()}

  @callback store_offset(
              GenServer.server(),
              stream_name :: String.t(),
              offset_reference :: String.t(),
              offset :: integer()
            ) :: :ok

  @callback query_offset(GenServer.server(), stream_name :: String.t(), offset_reference :: String.t()) ::
              {:ok, offset :: integer()} | {:error, reason :: atom()}

  @callback declare_producer(GenServer.server(), stream_name :: String.t(), producer_reference :: String.t()) ::
              {:ok, producer_id :: integer()} | {:error, reason :: atom()}

  @callback declare_producer(
              GenServer.server(),
              stream_name :: String.t(),
              producer_reference :: String.t(),
              producer_id :: integer()
            ) ::
              {:ok, producer_id :: integer()} | {:error, reason :: atom()}

  @callback delete_producer(GenServer.server(), producer_id :: integer()) ::
              :ok | {:error, reason :: atom()}

  @callback query_producer_sequence(GenServer.server(), stream_name :: String.t(), producer_reference :: String.t()) ::
              {:ok, sequence :: integer()} | {:error, reason :: atom()}

  @callback query_metadata(GenServer.server(), streams :: [String.t()]) ::
              {:ok, metadata :: QueryMetadataResponseData.t()}
              | {:error, reason :: atom()}

  @callback publish(
              GenServer.server(),
              producer_id :: integer(),
              publishing_id :: integer(),
              message :: binary(),
              filter_value :: binary() | nil
            ) ::
              :ok

  @callback subscribe(
              GenServer.server(),
              stream_name :: String.t(),
              pid :: pid(),
              offset :: RabbitMQStream.Connection.offset(),
              credit :: non_neg_integer(),
              properties :: Keyword.t()
            ) :: {:ok, subscription_id :: non_neg_integer()} | {:error, reason :: atom()}

  @callback unsubscribe(GenServer.server(), subscription_id :: non_neg_integer()) ::
              :ok | {:error, reason :: atom()}

  @callback respond(
              GenServer.server(),
              subscription_id :: non_neg_integer(),
              request :: RabbitMQStream.Message.Request.t(),
              opts :: Keyword.t()
            ) :: :ok

  @callback credit(GenServer.server(), subscription_id :: non_neg_integer(), credit :: non_neg_integer()) :: :ok

  @callback stream_stats(GenServer.server(), stream_name :: String.t()) ::
              {:ok, stats :: StreamStatsResponseData.t()}
              | {:error, reason :: atom()}

  @callback partitions(GenServer.server(), super_stream :: String.t()) ::
              {:ok, partitions :: PartitionsQueryResponseData.t()}
              | {:error, reason :: atom()}

  @callback route(GenServer.server(), routing_key :: String.t(), super_stream :: String.t()) ::
              :ok | {:error, reason :: atom()}
  @callback create_super_stream(
              GenServer.server(),
              name :: String.t(),
              partitions :: [String.t()],
              arguments :: keyword(String.t()) | nil
            ) ::
              :ok | {:error, reason :: atom()}

  @callback delete_super_stream(GenServer.server(), name :: String.t()) ::
              :ok | {:error, reason :: atom()}

  @callback supports?(GenServer.server(), command :: atom()) :: boolean()
  @callback supports?(GenServer.server(), command :: atom(), version :: String.t()) :: boolean()
end

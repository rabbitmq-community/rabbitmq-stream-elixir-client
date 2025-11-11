defmodule RabbitMQStream.Message do
  @moduledoc false
  require Logger

  alias RabbitMQStream.Connection

  alias RabbitMQStream.Message.Types

  defmodule Request do
    @moduledoc false
    @type t :: %__MODULE__{
            version: non_neg_integer,
            correlation_id: non_neg_integer,
            command: atom,
            data: term(),
            code: non_neg_integer
          }

    @enforce_keys [:command, :version]

    defstruct [
      :version,
      :correlation_id,
      :command,
      :data,
      :code
    ]
  end

  defmodule Response do
    @moduledoc false
    @type t :: %__MODULE__{
            version: non_neg_integer,
            correlation_id: non_neg_integer,
            command: atom,
            data: Types.t(),
            code: non_neg_integer
          }

    @enforce_keys [:command, :version]
    defstruct [
      :version,
      :command,
      :correlation_id,
      :data,
      :code
    ]
  end

  @version Mix.Project.config()[:version]

  def new_request(%Connection{} = conn, :peer_properties, _) do
    %Request{
      version: 1,
      correlation_id: conn.correlation_sequence,
      command: :peer_properties,
      data: %Types.PeerPropertiesData{
        peer_properties: [
          {"product", "RabbitMQ Stream Client"},
          {"information", "Development"},
          {"version", @version},
          {"platform", "Elixir"}
        ]
      }
    }
  end

  def new_request(%Connection{} = conn, :sasl_handshake, _) do
    %Request{
      version: 1,
      correlation_id: conn.correlation_sequence,
      command: :sasl_handshake,
      data: %Types.SaslHandshakeData{}
    }
  end

  def new_request(%Connection{} = conn, :sasl_authenticate, _) do
    cond do
      Enum.member?(conn.mechanisms, "PLAIN") ->
        %Request{
          version: 1,
          correlation_id: conn.correlation_sequence,
          command: :sasl_authenticate,
          data: %Types.SaslAuthenticateData{
            mechanism: "PLAIN",
            sasl_opaque_data: [
              username: conn.options[:username],
              password: conn.options[:password]
            ]
          }
        }

      true ->
        raise "Unsupported SASL mechanism: #{conn.mechanisms}"
    end
  end

  def new_request(%Connection{} = conn, :tune, _) do
    %Request{
      version: 1,
      correlation_id: conn.correlation_sequence,
      command: :tune,
      data: %Types.TuneData{
        frame_max: conn.options[:frame_max],
        heartbeat: conn.options[:heartbeat]
      }
    }
  end

  def new_request(%Connection{} = conn, :open, _) do
    %Request{
      version: 1,
      correlation_id: conn.correlation_sequence,
      command: :open,
      data: %Types.OpenRequestData{
        vhost: conn.options[:vhost]
      }
    }
  end

  def new_request(%Connection{}, :heartbeat, _) do
    %Request{
      version: 1,
      command: :heartbeat,
      data: %Types.HeartbeatData{}
    }
  end

  def new_request(%Connection{} = conn, :close, opts) do
    %Request{
      version: 1,
      command: :close,
      correlation_id: conn.correlation_sequence,
      data: %Types.CloseRequestData{
        code: opts[:code],
        reason: opts[:reason]
      }
    }
  end

  def new_request(%Connection{} = conn, :create_stream, opts) do
    %Request{
      version: 1,
      command: :create_stream,
      correlation_id: conn.correlation_sequence,
      data: %Types.CreateStreamRequestData{
        stream_name: opts[:name],
        arguments: opts[:arguments] || []
      }
    }
  end

  def new_request(%Connection{} = conn, :delete_stream, opts) do
    %Request{
      version: 1,
      command: :delete_stream,
      correlation_id: conn.correlation_sequence,
      data: %Types.DeleteStreamRequestData{
        stream_name: opts[:name]
      }
    }
  end

  def new_request(%Connection{} = conn, :store_offset, opts) do
    %Request{
      version: 1,
      command: :store_offset,
      correlation_id: conn.correlation_sequence,
      data: %Types.StoreOffsetRequestData{
        stream_name: opts[:stream_name],
        offset_reference: opts[:offset_reference],
        offset: opts[:offset]
      }
    }
  end

  def new_request(%Connection{} = conn, :query_offset, opts) do
    %Request{
      version: 1,
      command: :query_offset,
      correlation_id: conn.correlation_sequence,
      data: %Types.QueryOffsetRequestData{
        stream_name: opts[:stream_name],
        offset_reference: opts[:offset_reference]
      }
    }
  end

  def new_request(%Connection{} = conn, :declare_producer, opts) do
    %Request{
      version: 1,
      command: :declare_producer,
      correlation_id: conn.correlation_sequence,
      data: %Types.DeclareProducerRequestData{
        producer_id: opts[:producer_id],
        producer_reference: opts[:producer_reference],
        stream_name: opts[:stream_name]
      }
    }
  end

  def new_request(%Connection{} = conn, :delete_producer, opts) do
    %Request{
      version: 1,
      command: :delete_producer,
      correlation_id: conn.correlation_sequence,
      data: %Types.DeleteProducerRequestData{
        producer_id: opts[:producer_id]
      }
    }
  end

  def new_request(%Connection{} = conn, :query_metadata, opts) do
    %Request{
      version: 1,
      command: :query_metadata,
      correlation_id: conn.correlation_sequence,
      data: %Types.QueryMetadataRequestData{
        streams: opts[:streams]
      }
    }
  end

  def new_request(%Connection{} = conn, :query_producer_sequence, opts) do
    %Request{
      version: 1,
      command: :query_producer_sequence,
      correlation_id: conn.correlation_sequence,
      data: %Types.QueryProducerSequenceRequestData{
        stream_name: opts[:stream_name],
        producer_reference: opts[:producer_reference]
      }
    }
  end

  def new_request(%Connection{} = conn, :publish, opts) when conn.commands.publish.max >= 2 do
    %Request{
      version: 2,
      command: :publish,
      data: %Types.PublishData{
        producer_id: opts[:producer_id],
        messages: opts[:messages]
      }
    }
  end

  def new_request(%Connection{}, :publish, opts) do
    %Request{
      version: 1,
      command: :publish,
      data: %Types.PublishData{
        producer_id: opts[:producer_id],
        messages: opts[:messages]
      }
    }
  end

  def new_request(%Connection{} = conn, :subscribe, opts) do
    %Request{
      version: 1,
      command: :subscribe,
      correlation_id: conn.correlation_sequence,
      data: Types.SubscribeRequestData.new!(opts)
    }
  end

  def new_request(%Connection{} = conn, :unsubscribe, opts) do
    %Request{
      version: 1,
      command: :unsubscribe,
      correlation_id: conn.correlation_sequence,
      data: %Types.UnsubscribeRequestData{
        subscription_id: opts[:subscription_id]
      }
    }
  end

  def new_request(%Connection{} = conn, :credit, opts) do
    %Request{
      version: 1,
      command: :credit,
      correlation_id: conn.correlation_sequence,
      data: %Types.CreditRequestData{
        credit: opts[:credit],
        subscription_id: opts[:subscription_id]
      }
    }
  end

  def new_request(%Connection{} = conn, :route, opts) do
    %Request{
      version: 1,
      command: :route,
      correlation_id: conn.correlation_sequence,
      data: %Types.RouteRequestData{
        super_stream: opts[:super_stream],
        routing_key: opts[:routing_key]
      }
    }
  end

  def new_request(%Connection{} = conn, :partitions, opts) do
    %Request{
      version: 1,
      command: :partitions,
      correlation_id: conn.correlation_sequence,
      data: %Types.PartitionsQueryRequestData{
        super_stream: opts[:super_stream]
      }
    }
  end

  def new_request(%Connection{} = conn, :exchange_command_versions, opts) do
    %Request{
      version: 1,
      command: :exchange_command_versions,
      correlation_id: conn.correlation_sequence,
      data: Types.ExchangeCommandVersionsData.new!(opts)
    }
  end

  def new_request(%Connection{} = conn, :stream_stats, opts) do
    %Request{
      version: 1,
      command: :stream_stats,
      correlation_id: conn.correlation_sequence,
      data: %Types.StreamStatsRequestData{
        stream_name: opts[:stream_name]
      }
    }
  end

  def new_request(%Connection{} = conn, :create_super_stream, opts) do
    %Request{
      version: 1,
      command: :create_super_stream,
      correlation_id: conn.correlation_sequence,
      data: struct(Types.CreateSuperStreamRequestData, opts)
    }
  end

  def new_request(%Connection{} = conn, :delete_super_stream, opts) do
    %Request{
      version: 1,
      command: :delete_super_stream,
      correlation_id: conn.correlation_sequence,
      data: struct(Types.DeleteSuperStreamRequestData, opts)
    }
  end

  def new_response(%Connection{options: opts}, :tune, correlation_id: correlation_id) do
    %Response{
      version: 1,
      command: :tune,
      correlation_id: correlation_id,
      data: struct(Types.TuneData, opts)
    }
  end

  def new_response(%Connection{}, :heartbeat, correlation_id: correlation_id) do
    %Response{
      version: 1,
      command: :heartbeat,
      correlation_id: correlation_id,
      data: %Types.HeartbeatData{}
    }
  end

  def new_response(%Connection{}, :close, correlation_id: correlation_id, code: code) do
    %Response{
      version: 1,
      correlation_id: correlation_id,
      command: :close,
      data: %Types.CloseResponseData{},
      code: code
    }
  end

  def new_response(%Connection{}, :consumer_update, opts) do
    %Response{
      version: 1,
      correlation_id: opts[:correlation_id],
      command: :consumer_update,
      data: %Types.ConsumerUpdateResponseData{
        offset: opts[:offset]
      },
      code: opts[:code]
    }
  end
end

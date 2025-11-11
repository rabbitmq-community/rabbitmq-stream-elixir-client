defmodule RabbitMQStream.Message.Data do
  @moduledoc false
  alias RabbitMQStream.Message.Types
  alias RabbitMQStream.Message.{Response, Request}
  import RabbitMQStream.Message.Helpers

  def decode(%{command: :heartbeat}, ""), do: %Types.HeartbeatData{}
  def decode(%Response{command: :close}, ""), do: %Types.CloseResponseData{}

  def decode(%Response{command: :create_stream}, ""), do: %Types.CreateStreamResponseData{}
  def decode(%Response{command: :delete_stream}, ""), do: %Types.DeleteStreamResponseData{}
  def decode(%Response{command: :declare_producer}, ""), do: %Types.DeclareProducerResponseData{}
  def decode(%Response{command: :delete_producer}, ""), do: %Types.DeleteProducerResponseData{}
  def decode(%Response{command: :subscribe}, ""), do: %Types.SubscribeResponseData{}
  def decode(%Response{command: :unsubscribe}, ""), do: %Types.UnsubscribeResponseData{}
  def decode(%Response{command: :credit}, ""), do: %Types.CreditResponseData{}
  def decode(%Response{command: :store_offset}, ""), do: %Types.StoreOffsetResponseData{}
  def decode(%Response{command: :create_super_stream}, ""), do: %Types.CreateSuperStreamResponseData{}
  def decode(%Response{command: :delete_super_stream}, ""), do: %Types.DeleteSuperStreamResponseData{}

  def decode(%Request{command: :publish_confirm}, buffer) do
    <<producer_id::unsigned-integer-size(8), buffer::binary>> = buffer

    {"", publishing_ids} =
      decode_array(buffer, fn buffer, acc ->
        <<publishing_id::unsigned-integer-size(64), buffer::binary>> = buffer
        {buffer, [publishing_id] ++ acc}
      end)

    %Types.PublishConfirmData{producer_id: producer_id, publishing_ids: publishing_ids}
  end

  def decode(%Response{command: :publish_error}, buffer) do
    <<producer_id::unsigned-integer-size(8), buffer::binary>> = buffer

    {"", errors} =
      decode_array(buffer, fn buffer, acc ->
        <<
          publishing_id::unsigned-integer-size(64),
          code::unsigned-integer-size(16),
          buffer::binary
        >> = buffer

        entry = %Types.PublishErrorData.Error{
          code: decode_code(code),
          publishing_id: publishing_id
        }

        {buffer, [entry] ++ acc}
      end)

    %Types.PublishErrorData{producer_id: producer_id, errors: errors}
  end

  def decode(%Request{version: 1, command: :deliver}, buffer) do
    <<subscription_id::unsigned-integer-size(8), rest::binary>> = buffer

    osiris_chunk = RabbitMQStream.OsirisChunk.decode!(rest)

    %Types.DeliverData{subscription_id: subscription_id, osiris_chunk: osiris_chunk}
  end

  def decode(%Request{version: 2, command: :deliver}, buffer) do
    <<subscription_id::unsigned-integer-size(8), committed_offset::unsigned-integer-size(64), rest::binary>> = buffer

    osiris_chunk = RabbitMQStream.OsirisChunk.decode!(rest)

    %Types.DeliverData{
      subscription_id: subscription_id,
      committed_offset: committed_offset,
      osiris_chunk: osiris_chunk
    }
  end

  def decode(%{command: :query_metadata}, buffer) do
    {buffer, brokers} =
      decode_array(buffer, fn buffer, acc ->
        <<reference::unsigned-integer-size(16), buffer::binary>> = buffer

        <<size::integer-size(16), host::binary-size(size), buffer::binary>> = buffer

        <<port::unsigned-integer-size(32), buffer::binary>> = buffer

        data = %Types.QueryMetadataResponseData.BrokerData{
          reference: reference,
          host: host,
          port: port
        }

        {buffer, [data] ++ acc}
      end)

    {"", streams} =
      decode_array(buffer, fn buffer, acc ->
        <<
          size::integer-size(16),
          name::binary-size(size),
          code::unsigned-integer-size(16),
          leader::unsigned-integer-size(16),
          buffer::binary
        >> = buffer

        {buffer, replicas} =
          decode_array(buffer, fn buffer, acc ->
            <<replica::unsigned-integer-size(16), buffer::binary>> = buffer

            {buffer, [replica] ++ acc}
          end)

        data = %Types.QueryMetadataResponseData.StreamData{
          code: decode_code(code),
          name: name,
          leader: leader,
          replicas: replicas
        }

        {buffer, [data] ++ acc}
      end)

    %Types.QueryMetadataResponseData{brokers: brokers, streams: streams}
  end

  def decode(%Request{command: :close}, <<code::unsigned-integer-size(16), buffer::binary>>) do
    {"", reason} = decode_string(buffer)

    %Types.CloseRequestData{code: code, reason: reason}
  end

  def decode(%{command: :metadata_update}, <<code::unsigned-integer-size(16), buffer::binary>>) do
    {"", stream_name} = decode_string(buffer)

    %Types.MetadataUpdateData{stream_name: stream_name, code: code}
  end

  def decode(%{command: :query_offset}, <<offset::unsigned-integer-size(64)>>) do
    %Types.QueryOffsetResponseData{offset: offset}
  end

  def decode(%Response{command: :query_producer_sequence}, <<sequence::unsigned-integer-size(64)>>) do
    %Types.QueryProducerSequenceResponseData{sequence: sequence}
  end

  def decode(%{command: :peer_properties}, buffer) do
    {"", peer_properties} =
      decode_array(buffer, fn buffer, acc ->
        {buffer, key} = decode_string(buffer)
        {buffer, value} = decode_string(buffer)

        {buffer, [{key, value} | acc]}
      end)

    %Types.PeerPropertiesData{peer_properties: Map.new(peer_properties)}
  end

  def decode(%{command: :sasl_handshake}, buffer) do
    {"", mechanisms} =
      decode_array(buffer, fn buffer, acc ->
        {buffer, value} = decode_string(buffer)
        {buffer, [value | acc]}
      end)

    %Types.SaslHandshakeData{mechanisms: mechanisms}
  end

  def decode(%{command: :sasl_authenticate}, buffer) do
    %Types.SaslAuthenticateData{sasl_opaque_data: buffer}
  end

  def decode(%{command: :tune}, <<frame_max::unsigned-integer-size(32), heartbeat::unsigned-integer-size(32)>>) do
    %Types.TuneData{frame_max: frame_max, heartbeat: heartbeat}
  end

  def decode(%{command: :open}, buffer) do
    {"", connection_properties} =
      decode_array(buffer, fn buffer, acc ->
        {buffer, key} = decode_string(buffer)
        {buffer, value} = decode_string(buffer)

        {buffer, [{key, value} | acc]}
      end)

    %Types.OpenResponseData{connection_properties: Map.new(connection_properties)}
  end

  def decode(%Response{command: :route}, buffer) do
    {"", streams} =
      decode_array(buffer, fn buffer, acc ->
        {buffer, value} = decode_string(buffer)
        {buffer, [value | acc]}
      end)

    %Types.RouteResponseData{streams: streams}
  end

  def decode(%Response{command: :partitions}, buffer) do
    {"", streams} =
      decode_array(buffer, fn buffer, acc ->
        {buffer, value} = decode_string(buffer)
        {buffer, [value | acc]}
      end)

    %Types.PartitionsQueryResponseData{streams: streams}
  end

  def decode(%Response{command: :exchange_command_versions}, buffer) do
    {"", commands} =
      decode_array(buffer, fn buffer, acc ->
        <<
          key::unsigned-integer-size(16),
          min_version::unsigned-integer-size(16),
          max_version::unsigned-integer-size(16),
          rest::binary
        >> = buffer

        value = %Types.ExchangeCommandVersionsData.Command{
          key: decode_command(key),
          min_version: min_version,
          max_version: max_version
        }

        {rest, [value | acc]}
      end)

    %Types.ExchangeCommandVersionsData{commands: commands}
  end

  def decode(%Response{command: :consumer_update}, buffer) do
    {"", offset} = decode_offset(buffer)

    %Types.ConsumerUpdateResponseData{offset: offset}
  end

  def decode(%Request{command: :consumer_update}, buffer) do
    <<subscription_id::unsigned-integer-size(8), flag::unsigned-integer-size(8)>> = buffer

    %Types.ConsumerUpdateRequestData{subscription_id: subscription_id, active: flag == 1}
  end

  def decode(%Response{command: :stream_stats}, buffer) do
    {"", stats} =
      decode_array(buffer, fn buffer, acc ->
        {buffer, key} = decode_string(buffer)
        <<value::integer-size(64), buffer::binary>> = buffer

        {buffer, [{key, value} | acc]}
      end)

    %Types.StreamStatsResponseData{stats: Map.new(stats)}
  end

  def encode(%Response{command: :close}) do
    <<>>
  end

  def encode(%Request{command: :peer_properties, data: data}) do
    properties = encode_map(data.peer_properties)

    <<properties::binary>>
  end

  def encode(%Request{command: :sasl_handshake}) do
    <<>>
  end

  def encode(%Request{command: :sasl_authenticate, data: data}) do
    mechanism = encode_string(data.mechanism)

    credentials =
      encode_bytes("\u0000#{data.sasl_opaque_data[:username]}\u0000#{data.sasl_opaque_data[:password]}")

    <<mechanism::binary, credentials::binary>>
  end

  def encode(%Request{command: :open, data: data}) do
    vhost = encode_string(data.vhost)

    <<vhost::binary>>
  end

  def encode(%Request{command: :heartbeat}) do
    <<>>
  end

  def encode(%Request{command: :tune, data: data}) do
    <<data.heartbeat::unsigned-integer-size(32)>>
  end

  def encode(%Request{command: :close, data: data}) do
    reason = encode_string(data.reason)

    <<data.code::unsigned-integer-size(16), reason::binary>>
  end

  def encode(%Request{command: :create_stream, data: data}) do
    stream_name = encode_string(data.stream_name)
    arguments = encode_map(data.arguments)

    <<stream_name::binary, arguments::binary>>
  end

  def encode(%Request{command: :delete_stream, data: data}) do
    stream_name = encode_string(data.stream_name)

    <<stream_name::binary>>
  end

  def encode(%Request{command: :store_offset, data: data}) do
    offset_reference = encode_string(data.offset_reference)
    stream_name = encode_string(data.stream_name)

    <<
      offset_reference::binary,
      stream_name::binary,
      data.offset::unsigned-integer-size(64)
    >>
  end

  def encode(%Request{command: :query_offset, data: data}) do
    offset_reference = encode_string(data.offset_reference)
    stream_name = encode_string(data.stream_name)

    <<
      offset_reference::binary,
      stream_name::binary
    >>
  end

  def encode(%Request{command: :declare_producer, data: data}) do
    producer_reference = encode_string(data.producer_reference)
    stream_name = encode_string(data.stream_name)

    <<
      data.producer_id::unsigned-integer-size(8),
      producer_reference::binary,
      stream_name::binary
    >>
  end

  def encode(%Request{command: :delete_producer, data: data}) do
    <<data.producer_id::unsigned-integer-size(8)>>
  end

  def encode(%Request{command: :query_metadata, data: data}) do
    streams =
      data.streams
      |> Enum.map(&encode_string/1)
      |> encode_array()

    <<streams::binary>>
  end

  def encode(%Request{command: :query_producer_sequence, data: data}) do
    producer_reference = encode_string(data.producer_reference)
    stream_name = encode_string(data.stream_name)

    <<producer_reference::binary, stream_name::binary>>
  end

  def encode(%Request{version: 1, command: :publish, data: data}) do
    messages =
      encode_array(
        for {publishing_id, message, nil} <- data.messages do
          <<
            publishing_id::unsigned-integer-size(64),
            encode_bytes(message)::binary
          >>
        end
      )

    <<data.producer_id::unsigned-integer-size(8), messages::binary>>
  end

  def encode(%Request{version: 2, command: :publish, data: data}) do
    messages =
      encode_array(
        for {publishing_id, message, filter_value} <- data.messages do
          <<
            publishing_id::unsigned-integer-size(64),
            encode_string(filter_value)::binary,
            encode_bytes(message)::binary
          >>
        end
      )

    <<data.producer_id::unsigned-integer-size(8), messages::binary>>
  end

  def encode(%Request{command: :subscribe, data: data}) do
    stream_name = encode_string(data.stream_name)

    offset = encode_offset(data.offset)

    properties =
      Enum.reduce(data.properties, [], fn
        {:filter, entries}, acc ->
          filter =
            for {entry, i} <- Enum.with_index(entries) do
              {"filter.#{i}", entry}
            end

          filter ++ acc

        {:single_active_consumer, name}, acc ->
          [{"single-active-consumer", true}, {"name", name} | acc]

        {:super_stream, name}, acc ->
          [{"super-stream", name} | acc]

        {key, value}, acc ->
          [{String.replace("#{key}", "_", "-"), value} | acc]
      end)
      |> encode_map()

    <<
      data.subscription_id::unsigned-integer-size(8),
      stream_name::binary,
      offset::binary,
      data.credit::unsigned-integer-size(16),
      properties::binary
    >>
  end

  def encode(%Request{command: :unsubscribe, data: data}) do
    <<data.subscription_id::unsigned-integer-size(8)>>
  end

  def encode(%Request{command: :credit, data: data}) do
    <<
      data.subscription_id::unsigned-integer-size(8),
      data.credit::unsigned-integer-size(16)
    >>
  end

  def encode(%Request{command: :route, data: data}) do
    routing_key = encode_string(data.routing_key)
    super_stream = encode_string(data.super_stream)

    <<routing_key::binary, super_stream::binary>>
  end

  def encode(%Request{command: :partitions, data: data}) do
    super_stream = encode_string(data.super_stream)

    <<super_stream::binary>>
  end

  def encode(%Request{command: :exchange_command_versions, data: data}) do
    encode_array(
      for command <- data.commands do
        <<
          encode_command(command.key)::unsigned-integer-size(16),
          command.min_version::unsigned-integer-size(16),
          command.max_version::unsigned-integer-size(16)
        >>
      end
    )
  end

  def encode(%Response{command: :tune, data: data}) do
    <<
      data.frame_max::unsigned-integer-size(32),
      data.heartbeat::unsigned-integer-size(32)
    >>
  end

  def encode(%Response{command: :consumer_update, data: data, code: :ok}) do
    encode_offset(data.offset)
  end

  def encode(%Response{command: :consumer_update}) do
    # The server expects an offset even if the response is not :ok.
    # So we send a default one.
    encode_offset(:next)
  end

  def encode(%Request{command: :stream_stats, data: data}) do
    encode_string(data.stream_name)
  end

  def encode(%Request{command: :create_super_stream, data: data}) do
    name = encode_string(data.name)

    # A routing key can have multiple associated streams
    partitions =
      for {routing_key, streams} <- data.partitions,
          streams = List.wrap(streams),
          stream <- streams do
        {routing_key, stream}
      end

    {binding_keys, partitions} = Enum.unzip(partitions)

    partitions = encode_array(partitions, &encode_string/1)
    binding_keys = encode_array(binding_keys, &encode_string/1)

    arguments = encode_map(data.arguments)

    <<name::binary, partitions::binary, binding_keys::binary, arguments::binary>>
  end

  def encode(%Request{command: :delete_super_stream, data: data}) do
    encode_string(data.name)
  end
end

defmodule RabbitMQStream.Message.Decoder do
  @moduledoc false
  import RabbitMQStream.Message.Helpers
  alias RabbitMQStream.Message.Data

  alias RabbitMQStream.Message.{Response, Request}
  import Bitwise

  def decode(buffer) do
    <<key::unsigned-integer-size(16), version::unsigned-integer-size(16), buffer::binary>> = buffer

    command = decode_command(key)

    if (key &&& 0b1000_0000_0000_0000) > 0 do
      %Response{version: version, command: command}
    else
      %Request{version: version, command: command}
    end
    |> decode(buffer)
  end

  def decode(%Response{command: :credit} = response, buffer) do
    <<code::unsigned-integer-size(16), _subscription_id::unsigned-integer-size(8)>> = buffer

    response = %{response | code: decode_code(code)}

    %{response | data: Data.decode(response, "")}
  end

  def decode(%Response{command: command} = response, buffer)
      when command in [
             :close,
             :create_stream,
             :delete_stream,
             :declare_producer,
             :delete_producer,
             :subscribe,
             :unsubscribe,
             :query_offset,
             :query_producer_sequence,
             :peer_properties,
             :sasl_handshake,
             :sasl_authenticate,
             :open,
             :route,
             :partitions,
             :exchange_command_versions,
             :consumer_update,
             :stream_stats,
             :create_super_stream,
             :delete_super_stream
           ] do
    <<correlation_id::unsigned-integer-size(32), code::unsigned-integer-size(16), buffer::binary>> = buffer

    response = %{response | correlation_id: correlation_id, code: decode_code(code)}

    %{response | data: Data.decode(response, buffer)}
  end

  def decode(%{command: command} = response, buffer)
      when command in [:close, :query_metadata, :consumer_update] do
    <<correlation_id::unsigned-integer-size(32), buffer::binary>> = buffer

    response = %{response | correlation_id: correlation_id}
    %{response | data: Data.decode(response, buffer)}
  end

  def decode(%{command: command} = action, buffer)
      when command in [
             :tune,
             :heartbeat,
             :metadata_update,
             :publish_confirm,
             :publish_error,
             :deliver,
             :store_offset
           ] do
    %{action | data: Data.decode(action, buffer)}
  end
end

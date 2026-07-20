defmodule RabbitMQStream.Message.DecoderTest do
  use ExUnit.Case, async: true

  alias RabbitMQStream.Message.{Decoder, Response}
  alias RabbitMQStream.Message.Types

  # Exact prod CreditResponse frame (minus the 4-byte length prefix):
  # key 0x8009, version 1, response_code 0x11 (precondition_failed), subscription_id 7.
  @credit_response <<0x8009::16, 1::16, 0x11::16, 7::8>>

  test "decodes a CreditResponse as a non-correlated response without crashing" do
    result = Decoder.decode(@credit_response)

    assert %Response{command: :credit, code: :precondition_failed} = result
    assert result.correlation_id == nil
    assert result.data == %Types.CreditResponseData{}
  end
end

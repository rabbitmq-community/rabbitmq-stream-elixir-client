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

  test "parse_frames drops an undecodable frame and keeps valid ones" do
    alias RabbitMQStream.Message.Buffer

    # 0x00FF is not a known command -> Decoder.decode raises. It must be skipped.
    bad = <<0x00FF::16, 1::16, 0x00>>
    good = @credit_response

    # Buffer stores frames reversed (prepended); pass [good, bad] so processing
    # order is bad then good.
    queue = Buffer.parse_frames([good, bad], :queue.new())
    commands = :queue.to_list(queue)

    assert [%Response{command: :credit}] = commands
  end
end

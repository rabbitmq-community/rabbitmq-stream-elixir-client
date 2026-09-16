defmodule RabbitMQStreamTest.ConnectionHandler do
  use ExUnit.Case, async: true

  alias RabbitMQStream.Connection
  alias RabbitMQStream.Connection.Handler
  alias RabbitMQStream.Message.{Data, Request, Response}
  alias RabbitMQStream.Message.Types.{PublishConfirmData, PublishErrorData}
  alias RabbitMQStream.Message.Types.PublishErrorData.Error

  defp conn(producers) do
    %Connection{
      transport: RabbitMQStream.Connection.Transport.TCP,
      options: [],
      state: :open,
      producers: producers
    }
  end

  test ":publish_confirm is routed to the pid registered for that producer_id" do
    request = %Request{
      version: 1,
      command: :publish_confirm,
      data: %PublishConfirmData{producer_id: 7, publishing_ids: [1, 2, 3]}
    }

    assert %Connection{} = Handler.handle_message(conn(%{7 => self()}), request)
    assert_receive {:publish_confirm, [1, 2, 3]}
  end

  test ":publish_error is routed to the pid registered for that producer_id" do
    error = %Error{publishing_id: 42, code: :stream_does_not_exist}

    request = %Request{
      version: 1,
      command: :publish_error,
      data: %PublishErrorData{producer_id: 9, errors: [error]}
    }

    assert %Connection{} = Handler.handle_message(conn(%{9 => self()}), request)
    assert_receive {:publish_error, [^error]}
  end

  test "an unknown producer_id is a no-op, not a crash" do
    request = %Request{
      version: 1,
      command: :publish_confirm,
      data: %PublishConfirmData{producer_id: 1, publishing_ids: [1]}
    }

    assert %Connection{} = Handler.handle_message(conn(%{}), request)
    refute_receive {:publish_confirm, _}
  end

  test "a :delete_producer response drops only that producer_id from producers and replies to the caller" do
    ref = make_ref()
    from = {self(), ref}
    correlation_id = 1

    conn_with_tracker = %{
      conn(%{5 => self(), 6 => self()})
      | request_tracker: %{{:delete_producer, correlation_id} => {from, 5}}
    }

    response = %Response{version: 1, command: :delete_producer, correlation_id: correlation_id}

    result = Handler.handle_message(conn_with_tracker, response)

    assert result.producers == %{6 => self()}
    assert_receive {^ref, :ok}
  end

  # Regression test: `Data.decode/2`'s `:publish_error` clause used to be guarded on
  # `%Response{command: :publish_error}`, but the decoder only ever produces a
  # `%Request{}` for this unsolicited, server-pushed frame (same as `:deliver` and
  # `:publish_confirm`) -- so a real `:publish_error` frame would raise
  # FunctionClauseError instead of decoding.
  test "a :publish_error frame decodes as a %Request{}, not a %Response{}" do
    # producer_id (8 bits) ++ array[1] of {publishing_id: 64 bits, code: 16 bits}
    buffer =
      <<9::unsigned-integer-size(8), 1::integer-size(32), 42::unsigned-integer-size(64),
        0x02::unsigned-integer-size(16)>>

    assert %PublishErrorData{
             producer_id: 9,
             errors: [%Error{publishing_id: 42, code: :stream_does_not_exist}]
           } = Data.decode(%Request{version: 1, command: :publish_error}, buffer)
  end
end

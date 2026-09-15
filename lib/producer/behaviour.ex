defmodule RabbitMQStream.Producer.Behaviour do
  @doc """
  Publishes a single message to the stream.

  The message first passes through the 'encode!/1' callback, before
  being sent to the RabbitMQStream.Producer process responsible for
  sending the binary message to the Connection.

  As of RabbitMQ 3.13.x, the mesagge's 'filter_value' is generated
  by passing it through the 'filter_value/1' callback.

  The callback always returns ':ok', as the server only send a response
  for a publish in case of an error, in which case the error code is logged.
  """
  @callback publish(message :: term()) :: :ok

  @doc """
  Callback responsible for encoding a message into binary format.

  It can be used in many ways, such as to 'Jason.encode!/1' a Map,
  or to Gzip Compact a message before it is appended to the Stream.

  """
  @callback encode!(message :: term()) :: binary()

  @doc """
  Optional callback that is called after the process has started, but before the
  producer has declared itself and fetched its most recent `publishing_id`.

  This is usefull for setup logic, such as creating the Stream if it doesn't yet exists.
  """
  @callback before_start(opts :: RabbitMQStream.Producer.options(), state :: RabbitMQStream.Producer.t()) ::
              RabbitMQStream.Producer.t()

  @doc """
  Callback responsible for generating the 'filter_value' for a message. The value
  is used by the Server for filtering the chunks sent to consumer that have defined
  a 'filter' parameter.

  The callback is invoked before the message is encoded, by the serializer options.

  Example defintion:
        @impl true
        def filter_value(message) do
          message["key"]
        end

  The default implementation defines `nil` as the `filter_value` for all messages.
  """
  @callback filter_value(message :: term()) :: String.t() | nil

  @doc """
  Optional callback invoked when the server confirms that a batch of previously
  published messages has been durably written to the stream.

  Receives the `publishing_id`s (as set at publish time) of every message confirmed
  in this batch.
  """
  @callback handle_confirm(publishing_ids :: [non_neg_integer()]) :: term()

  @doc """
  Optional callback invoked when the server reports that a batch of previously
  published messages failed to be written to the stream.

  Receives the list of `%RabbitMQStream.Message.Types.PublishErrorData.Error{}`
  structs (`publishing_id` and `code`) for this batch.
  """
  @callback handle_publish_error(errors :: [RabbitMQStream.Message.Types.PublishErrorData.Error.t()]) :: term()

  @optional_callbacks [before_start: 2, filter_value: 1, handle_confirm: 1, handle_publish_error: 1]
end

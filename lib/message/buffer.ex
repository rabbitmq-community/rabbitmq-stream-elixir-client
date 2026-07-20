defmodule RabbitMQStream.Message.Buffer do
  @moduledoc false
  alias RabbitMQStream.Message.Decoder

  # The code in this module is a one to one translation of the RabbitMQ decoding code at
  # https://github.com/rabbitmq/rabbitmq-server/blob/main/deps/rabbitmq_stream_common/src/rabbit_stream_core.erl

  defstruct commands: :queue.new(),
            frames: [],
            cfg: %{},
            data: nil

  @type t :: %__MODULE__{
          commands: :queue.queue(),
          frames: [iodata()],
          cfg: %{atom() => any()},
          data: nil | binary() | {non_neg_integer(), iodata()}
        }

  def init() do
    struct(__MODULE__)
  end

  def next_command(%__MODULE__{commands: commands} = state) do
    case :queue.out(commands) do
      {{:value, command}, commands} ->
        {command, %{state | commands: commands}}

      {:empty, _} ->
        {nil, state}
    end
  end

  def all_commands(%__MODULE__{commands: commands} = state) do
    {:queue.to_list(commands), %{state | commands: :queue.new()}}
  end

  @doc """
    Receives the binary, parses it into a command if possible, buffering when necessary.
  """
  def incoming_data(<<>>, %__MODULE__{frames: frames, commands: commands} = state) do
    %{state | frames: [], commands: parse_frames(frames, commands)}
  end

  def incoming_data(
        <<size::unsigned-integer-size(32), frame::binary-size(size), rest::binary>>,
        %__MODULE__{frames: frames, data: nil} = state
      ) do
    incoming_data(rest, %{state | frames: [frame | frames], data: nil})
  end

  def incoming_data(
        <<size::unsigned-integer-size(32), rest::binary>>,
        %__MODULE__{frames: frames, data: nil, commands: commands} = state
      ) do
    # not enough data to complete frame, stash and await more data
    %{state | frames: [], data: {size - byte_size(rest), rest}, commands: parse_frames(frames, commands)}
  end

  def incoming_data(data, %__MODULE__{frames: frames, data: nil, commands: commands} = state)
      when byte_size(data) < 4 do
    # not enough data to even know the size required
    # just stash binary and hit last clause next
    %{state | frames: [], data: data, commands: parse_frames(frames, commands)}
  end

  def incoming_data(
        data,
        %__MODULE__{frames: frames, data: {size, partial}, commands: commands} = state
      ) do
    case data do
      <<part::binary-size(size), rest::binary>> ->
        incoming_data(
          rest,
          %{state | frames: [<<partial::binary, part::binary>> | frames], data: nil}
        )

      rest ->
        %{
          state
          | frames: [],
            data: {size - byte_size(rest), <<partial::binary, rest::binary>>},
            commands: parse_frames(frames, commands)
        }
    end
  end

  def incoming_data(data, %__MODULE__{data: partial} = state) when is_binary(partial) do
    incoming_data(<<partial::binary, data::binary>>, %{state | data: nil})
  end

  def incoming_data(data, _state) do
    throw("Unhandled data: #{inspect(data)}")
  end

  def parse_frames(frames, queue) do
    # We receive the frames in the reverse order that they were received,
    # since we are prepending them to the list. So here we reverse the it to
    # correctly insert them into the queue.
    #
    # This is different from the reference implementation, but necessary to
    # maintain the order of commands received. The RabbitMQ's implementation
    # might actually be wrong, since it doesn't seem to be used anywhere, and
    # there are no tests for multiple frames in a single message.
    frames
    |> Enum.reverse()
    |> Enum.reduce(queue, fn frame, acc ->
      try do
        :queue.in(Decoder.decode(frame), acc)
      rescue
        error ->
          require Logger

          Logger.warning("RabbitMQStream: dropping undecodable frame",
            error: inspect(error),
            frame: inspect(frame, limit: 64)
          )

          acc
      end
    end)
  end
end

defmodule RabbitMQStream.StreamReader do
  defstruct [
    :batch_size,
    :connection,
    :stream_name,
    :offset,
    subcription_id: nil,
    buffer: :queue.new(),
    pending: nil
  ]

  use GenServer

  @impl true
  def init(opts \\ []) do
    {:ok, struct(__MODULE__, opts), {:continue, {:init, opts}}}
  end

  @impl true
  def handle_continue({:init, opts}, %__MODULE__{} = state) do
    {:ok, subcription_id} =
      RabbitMQStream.Connection.subscribe(
        state.connection,
        state.stream_name,
        self(),
        state.offset,
        state.batch_size * 2,
        opts[:properties] || []
      )

    {:noreply, %{state | subcription_id: subcription_id}}
  end

  @impl true
  def handle_info(
        {:deliver, %{osiris_chunk: %RabbitMQStream.OsirisChunk{data_entries: messages}}},
        %__MODULE__{} = state
      ) do
    state = %{state | buffer: :queue.join(state.buffer, :queue.from_list(messages))}

    if :queue.len(state.buffer) >= state.batch_size do
      :ok = RabbitMQStream.Connection.credit(state.connection, state.subcription_id, state.batch_size)
    end

    if state.pending != nil do
      case :queue.out(state.buffer) do
        {{:value, response}, queue} ->
          GenServer.reply(state.pending, response)

          {:noreply, %{state | buffer: queue, pending: nil}}

        {:empty, _} ->
          {:noreply, state}
      end
    else
      {:noreply, state}
    end
  end

  @impl true
  def handle_call(:read, from, state) do
    case :queue.out(state.buffer) do
      {{:value, response}, queue} ->
        {:reply, response, %{state | buffer: queue}}

      {:empty, _} ->
        {:noreply, %{state | pending: from}}
    end
  end

  def stream!(opts) do
    Stream.resource(
      fn ->
        {:ok, pid} = GenServer.start_link(__MODULE__, opts)
        pid
      end,
      fn pid ->
        case GenServer.call(pid, :read, 50000) do
          nil ->
            {:halt, pid}

          data ->
            {[data], pid}
        end
      end,
      &GenServer.stop/1
    )
  end
end

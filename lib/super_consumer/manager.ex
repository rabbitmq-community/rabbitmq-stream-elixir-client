defmodule RabbitMQStream.SuperConsumer.Manager do
  @moduledoc false
  alias RabbitMQStream.SuperConsumer
  alias RabbitMQStream.SuperStream.Helpers

  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: opts[:name])
  end

  @impl true
  def init(opts \\ []) do
    state = struct(SuperConsumer, opts)

    {:ok, state, {:continue, :start}}
  end

  @impl true
  def handle_continue(:start, %SuperConsumer{} = state) do
    partitions = Helpers.get_partitions(state.connection, state.super_stream, state.partitions)

    for partition <- partitions do
      {:ok, _pid} =
        DynamicSupervisor.start_child(
          state.dynamic_supervisor,
          {
            RabbitMQStream.Consumer,
            Keyword.merge(state.consumer_opts,
              name: {:via, Registry, {state.registry, partition}},
              stream_name: partition,
              consumer_module: state.consumer_module,
              properties: [
                single_active_consumer: state.super_stream,
                super_stream: state.super_stream
              ]
            )
          }
        )
    end

    {:noreply, state, :hibernate}
  end
end

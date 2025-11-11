defmodule RabbitMQStream.Producer.LifeCycle do
  @moduledoc false
  use GenServer

  # Callbacks
  @impl GenServer
  def init(opts \\ []) do
    reference_name = Keyword.get(opts, :reference_name, Atom.to_string(opts[:producer_module]))
    connection = Keyword.get(opts, :connection) || raise(":connection is required")
    stream_name = Keyword.get(opts, :stream_name) || raise(":stream_name is required")

    state = %RabbitMQStream.Producer{
      id: nil,
      sequence: nil,
      stream_name: stream_name,
      connection: connection,
      reference_name: reference_name,
      producer_module: opts[:producer_module]
    }

    {:ok, state, {:continue, opts}}
  end

  @impl GenServer
  def handle_continue(opts, state) do
    state =
      if state.producer_module != nil and function_exported?(state.producer_module, :before_start, 2) do
        apply(state.producer_module, :before_start, [opts, state])
      else
        state
      end

    with {:ok, id} <-
           RabbitMQStream.Connection.declare_producer(state.connection, state.stream_name, state.reference_name),
         {:ok, sequence} <-
           RabbitMQStream.Connection.query_producer_sequence(state.connection, state.stream_name, state.reference_name) do
      {:noreply, %{state | id: id, sequence: sequence + 1}}
    else
      err ->
        {:stop, err, state}
    end
  end

  @impl GenServer
  def handle_cast({:publish, {message, filter_value}}, %RabbitMQStream.Producer{} = state) when is_binary(message) do
    :ok = RabbitMQStream.Connection.publish(state.connection, state.id, state.sequence, message, filter_value)

    {:noreply, %{state | sequence: state.sequence + 1}}
  end

  @impl GenServer
  def handle_info({:redeclare_producer, _opts}, state) do
    # Since the connection has changed, we need to update the producer's own ID. This shouldn't be
    # an issue.
    with {:ok, id} <-
           RabbitMQStream.Connection.declare_producer(state.connection, state.stream_name, state.reference_name) do
      # The sequence number should have changed
      {:noreply, %{state | id: id}}
    else
      err ->
        {:stop, err, state}
    end
  end

  @impl GenServer
  def terminate(_reason, %{id: nil}), do: :ok

  def terminate(_reason, state) do
    RabbitMQStream.Connection.delete_producer(state.connection, state.id)
    :ok
  end
end

defmodule RabbitMQStream.Consumer.LifeCycle do
  @moduledoc false
  alias RabbitMQStream.Message.{Request, Types.DeliverData}
  alias RabbitMQStream.Consumer.{FlowControl, OffsetTracking}
  alias RabbitMQStream.Connection.Router

  use GenServer
  require Logger

  @impl true
  def init(opts \\ []) do
    opts =
      opts
      |> Keyword.put_new(:initial_credit, 50_000)
      |> Keyword.put_new(:offset_tracking, count: [store_after: 50])
      |> Keyword.put_new(:flow_control, count: [credit_after: {:count, 1}])
      |> Keyword.put_new(:offset_reference, Atom.to_string(opts[:consumer_module]))
      |> Keyword.put_new(:properties, [])

    unless opts[:initial_offset] != nil do
      raise "initial_offset is required"
    end

    # Prevent startup if 'single_active_consumer' is active, but there is no
    # handle_update/2 callback defined.
    if Keyword.get(opts[:properties], :single_active_consumer) != nil do
      if not function_exported?(opts[:consumer_module], :handle_update, 2) do
        raise "handle_update/2 must be implemented when using ':single_active_consumer' property"
      end
    end

    # Prevent startup if either 'filter' or 'match_unfiltered' is active, but there is no
    # filter_value/1 callback defined.
    if Keyword.get(opts[:properties], :filter) != nil or Keyword.get(opts[:properties], :match_unfiltered) != nil do
      if not function_exported?(opts[:consumer_module], :filter_value, 1) do
        raise "filter_value/1 must be implemented when using ':filter' or ':match_unfiltered' properties"
      end
    end

    opts =
      opts
      |> Keyword.put(:offset_tracking, OffsetTracking.init(opts[:offset_tracking], opts))
      |> Keyword.put(:flow_control, FlowControl.init(opts[:flow_control], opts))
      |> Keyword.put(:credits, opts[:initial_credit])
      |> Keyword.put(:seed_connection, opts[:connection])

    state = struct(RabbitMQStream.Consumer, opts)

    {:ok, state, {:continue, {:init, opts}}}
  end

  @impl true
  def handle_continue({:init, opts}, state) do
    # `before_start/2` runs first, unchanged from before this feature existed — it's
    # commonly used to create the stream itself, so leader/replica resolution (which
    # needs the stream to already exist) is deliberately done afterwards, not before.
    state =
      if function_exported?(state.consumer_module, :before_start, 2) do
        apply(state.consumer_module, :before_start, [opts, state])
      else
        state
      end

    state = resolve_connection(state)

    last_offset =
      case RabbitMQStream.Connection.query_offset(state.connection, state.stream_name, state.offset_reference) do
        {:ok, offset} ->
          Logger.debug(
            "#{state.consumer_module}: Fetched current consumer offset from the stream. Using it as the initial offset: #{offset}"
          )

          {:offset, offset}

        _ ->
          Logger.debug(
            "#{state.consumer_module}: No offset found for the consumer. Using default initial offset: #{inspect(opts[:initial_offset])}"
          )

          opts[:initial_offset]
      end

    case RabbitMQStream.Connection.subscribe(
           state.connection,
           state.stream_name,
           self(),
           last_offset,
           state.initial_credit,
           state.properties
         ) do
      {:ok, id} ->
        last_offset =
          case last_offset do
            {:offset, offset} ->
              offset

            _ ->
              0
          end

        {:noreply, %{state | id: id, last_offset: last_offset}}

      err ->
        {:stop, err, state}
    end
  end

  @impl true
  def terminate(_reason, %{id: nil}), do: :ok

  def terminate(_reason, state) do
    Logger.debug(
      "#{state.consumer_module}: Storing Offset and unsubscribing, during termination. Last offset: #{state.last_offset}"
    )

    # While not guaranteed, we attempt to store the offset when terminating. Useful for when performing
    # upgrades, and in a 'single-active-consumer' scenario.
    if state.last_offset != 0 do
      RabbitMQStream.Connection.store_offset(
        state.connection,
        state.stream_name,
        state.offset_reference,
        state.last_offset
      )
    end

    RabbitMQStream.Connection.unsubscribe(state.connection, state.id)
    :ok
  end

  @impl true
  def handle_info({:deliver, %DeliverData{osiris_chunk: %RabbitMQStream.OsirisChunk{} = chunk}}, state) do
    if function_exported?(state.consumer_module, :handle_chunk, 2) do
      apply(state.consumer_module, :handle_chunk, [chunk, state])
    end

    for message <- Enum.drop(chunk.data_entries, max(state.last_offset - chunk.chunk_id, 0)) do
      message =
        if function_exported?(state.consumer_module, :decode!, 1) do
          apply(state.consumer_module, :decode!, [message])
        else
          message
        end

      if filtered?(message, state) do
        if function_exported?(state.consumer_module, :handle_message, 3) do
          apply(state.consumer_module, :handle_message, [message, chunk, state])
        end

        if function_exported?(state.consumer_module, :handle_message, 2) do
          apply(state.consumer_module, :handle_message, [message, state])
        end

        if function_exported?(state.consumer_module, :handle_message, 1) do
          apply(state.consumer_module, :handle_message, [message])
        end
      end
    end

    # Based on the [Python implementation](https://github.com/qweeze/rstream/blob/a81176a5c7cf4accaee25ca7725bd7bd94bf0ce8/rstream/consumer.py#L327),
    # it seems to be OK to sum the amount of messages received to the chunk_id, which represents
    # the offset of the first message, to get the new `last_offset` for the messages.
    # During the second iteration of this logic to get the `last_offset`, I attempted to use the
    # `commited_offset` of the DeliveryData, thinking it was already the offset for the `next` messsage,
    # but it isn't.
    state = %{state | last_offset: chunk.chunk_id + chunk.num_entries}

    offset_tracking =
      for {strategy, track_state} <- state.offset_tracking do
        if function_exported?(strategy, :after_chunk, 3) do
          {strategy, strategy.after_chunk(track_state, chunk, state)}
        else
          {strategy, track_state}
        end
      end

    state = %{state | offset_tracking: offset_tracking, credits: state.credits - chunk.num_entries}

    state = state |> OffsetTracking.run() |> FlowControl.run()

    {:noreply, state}
  end

  def handle_info(:run_offset_tracking, state) do
    {:noreply, OffsetTracking.run(state)}
  end

  def handle_info(:run_flow_control, state) do
    {:noreply, FlowControl.run(state)}
  end

  def handle_info({:command, %Request{command: :consumer_update, data: data} = request}, state) do
    if function_exported?(state.consumer_module, :handle_update, 2) do
      action = if data.active, do: :upgrade, else: :downgrade

      # It seems that sending an offset to the server when a consumer is being
      # downgraded from 'active' to 'inactive' has no effect. But the server
      # still waits a response. A consumer module might use this downgrade
      # to send the current offset to the consumer that is being upgraded.

      case apply(state.consumer_module, :handle_update, [state, action]) do
        {:ok, offset} ->
          Logger.debug("Consumer upgraded to active consumer")
          RabbitMQStream.Connection.respond(state.connection, request, offset: offset, code: :ok)

        {:error, reason} ->
          Logger.error("Error updating consumer: #{inspect(reason)}")
          RabbitMQStream.Connection.respond(state.connection, request, code: :internal_error)
      end
    else
      Logger.error("handle_update/2 must be implemented when using single-active-consumer property")
      RabbitMQStream.Connection.respond(state.connection, request, code: :internal_error)
    end

    {:noreply, state}
  end

  @impl true
  def handle_cast({:credit, amount}, state) do
    RabbitMQStream.Connection.credit(state.connection, state.id, amount)
    {:noreply, %{state | credits: state.credits + amount}}
  end

  def handle_cast(:store_offset, state) do
    RabbitMQStream.Connection.store_offset(
      state.connection,
      state.stream_name,
      state.offset_reference,
      state.last_offset
    )

    {:noreply, state}
  end

  @impl true
  def handle_call(:get_credits, _from, state) do
    {:reply, state.credits, state}
  end

  # Resolves the stream's leader/replica and swaps `state.connection` to it. Any
  # resolution failure (stream not found yet, metadata unreachable, etc.) is
  # non-fatal: it just keeps using whatever connection is already set, so a consumer
  # that can't resolve leader routing behaves exactly as it did before this feature
  # existed. The resolved connection is stored back into `state.connection` and used
  # consistently for the rest of this process's life — required by `respond/3` in the
  # `consumer_update` handler below, which must reply over the same connection the
  # request arrived on.
  defp resolve_connection(state) do
    case Router.consumer_connection(state.seed_connection, state.stream_name) do
      {:ok, connection} -> %{state | connection: connection}
      {:error, _reason} -> state
    end
  end

  @doc false
  defp filtered?(decoded, %RabbitMQStream.Consumer{} = state) do
    filter = state.properties[:filter]
    match_unfiltered = state.properties[:match_unfiltered]

    if filter != nil or match_unfiltered != nil do
      filter_value =
        if function_exported?(state.consumer_module, :filter_value, 1) do
          apply(state.consumer_module, :filter_value, [decoded])
        else
          nil
        end

      case {filter_value, filter, match_unfiltered} do
        {nil, _, true} ->
          true

        {_, _, true} ->
          false

        {filter_value, entries, _} when is_list(entries) ->
          Enum.member?(entries, filter_value)
      end
    else
      true
    end
  end
end

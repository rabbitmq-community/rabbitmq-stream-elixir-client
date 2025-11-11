defmodule RabbitMQStream.Consumer.LifeCycle do
  @moduledoc false
  alias RabbitMQStream.Message.{Request, Types.DeliverData}
  alias RabbitMQStream.Consumer.{FlowControl, OffsetTracking}

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

    state = struct(RabbitMQStream.Consumer, opts)

    {:ok, state, {:continue, {:init, opts}}}
  end

  @impl true
  def handle_continue({:init, opts}, state) do
    state =
      if function_exported?(state.consumer_module, :before_start, 2) do
        apply(state.consumer_module, :before_start, [opts, state])
      else
        state
      end

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
              # We don't need to default to anything other than '0' here since any possibly duplicate
              # messages received after a 'resubscribe' would be filtered by the ':deliver' handler
              # bellow, since it persists the 'last_offset' of each chunk.
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
  # The 'resubscribe' flow is not necessarily the same as 'init'.
  def handle_info({:resubscribe, _args}, state) do
    # The 'args' we receive are the ones we
    Logger.info("Connection has shut down. Re-executing 'subscribe' flow")

    # If we had a 'last_offset' set, we should to use it.
    last_offset =
      if state.last_offset != 0 do
        {:offset, state.last_offset}
      else
        # Or else, we should follow the same setup flow of fetching the offset from the stream
        case RabbitMQStream.Connection.query_offset(state.connection, state.stream_name, state.offset_reference) do
          {:ok, offset} ->
            {:offset, offset}

          _ ->
            state.initial_offset
        end
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

        # A consumer should always update its own id
        {:noreply, %{state | id: id, last_offset: last_offset}}

      err ->
        {:stop, err, state}
    end
  end

  def handle_info({:deliver, %DeliverData{osiris_chunk: %RabbitMQStream.OsirisChunk{} = chunk}}, state) do
    # If some of the messages in the chunk we haven't yet processed, we can go forward with the processing.
    # We do this check since a connection can fail, and be automatically reset by the client. In this
    # case we fast forward any repeated messages since the last offset store
    if chunk.chunk_id + chunk.num_entries > state.last_offset do
      if function_exported?(state.consumer_module, :handle_chunk, 2) do
        apply(state.consumer_module, :handle_chunk, [chunk, state])
      end

      for message <- Enum.slice(chunk.data_entries, (state.last_offset - chunk.chunk_id)..chunk.num_entries) do
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
    else
      Logger.debug(
        "#{state.consumer_module}: Received already processed chunk, possibly due to reconnection. Fast fowarding."
      )

      {:noreply, state}
    end
  end

  def handle_info(:run_offset_tracking, state) do
    {:noreply, OffsetTracking.run(state)}
  end

  def handle_info(:run_flow_control, state) do
    {:noreply, FlowControl.run(state)}
  end

  def handle_info(
        {:command, %Request{command: :consumer_update, data: data} = request},
        %RabbitMQStream.Consumer{} = state
      ) do
    if function_exported?(state.consumer_module, :handle_update, 2) do
      action = if data.active, do: :upgrade, else: :downgrade

      # It seems that sending an offset to the server when a consumer is being
      # downgraded from 'active' to 'inactive' has no effect. But the server
      # still waits a response. A consumer module might use this downgrade
      # to send the current offset to the consumer that is being upgraded.

      case apply(state.consumer_module, :handle_update, [state, action]) do
        {:ok, offset} ->
          Logger.debug("Consumer upgraded to active consumer")
          RabbitMQStream.Connection.respond(state.connection, state.id, request, offset: offset, code: :ok)

        {:error, reason} ->
          Logger.error("Error updating consumer: #{inspect(reason)}")
          RabbitMQStream.Connection.respond(state.connection, state.id, request, code: :internal_error)
      end
    else
      Logger.error("handle_update/2 must be implemented when using single-active-consumer property")
      RabbitMQStream.Connection.respond(state.connection, state.id, request, code: :internal_error)
    end

    {:noreply, state}
  end

  @impl true
  def handle_cast({:credit, amount}, state) do
    # Should be sent to same connection
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

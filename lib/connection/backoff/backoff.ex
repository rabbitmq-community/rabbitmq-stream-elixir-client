defmodule RabbitMQStream.Connection.Backoff do
  @type t :: module()

  @moduledoc """
  Behavior for connection-drop reconnect backoff strategies.

  # Existing Strategies
  You can use the default strategy by passing a shorthand alias:

  * `exponential` : `RabbitMQStream.Connection.Backoff.Exponential`

  """

  @doc """
  Initializes the strategy state.

  # Parameters
  * `opts` - a keyword list of options passed to the strategy.
  """
  @callback init(opts :: term()) :: term()

  @doc """
  Called after each failed (re)connect attempt to decide when to retry.

  # Parameters

  * `state` - the state of the strategy
  * `attempt` - the 1-indexed number of the attempt that just failed
  """
  @callback next_delay(state :: term(), attempt :: pos_integer()) ::
              {delay_ms :: non_neg_integer(), state :: term()} | :stop

  @defaults %{
    exponential: RabbitMQStream.Connection.Backoff.Exponential
  }

  @doc false
  def init({strategy, opts}) do
    strategy = @defaults[strategy] || strategy

    {strategy, strategy.init(opts)}
  end

  def init(strategy) do
    strategy = @defaults[strategy] || strategy

    {strategy, strategy.init([])}
  end

  @doc false
  def next_delay({strategy, state}, attempt) do
    case strategy.next_delay(state, attempt) do
      {delay, state} -> {delay, {strategy, state}}
      :stop -> :stop
    end
  end
end

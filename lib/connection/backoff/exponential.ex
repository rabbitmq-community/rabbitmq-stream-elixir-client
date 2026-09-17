defmodule RabbitMQStream.Connection.Backoff.Exponential do
  @behaviour RabbitMQStream.Connection.Backoff

  @moduledoc """
  Exponential backoff strategy, with full jitter, used to space out reconnect
  attempts after a connection drop. Retries indefinitely -- never gives up.

  # Parameters

  * `initial` - the delay, in milliseconds, before the first retry. Defaults to `1_000`.
  * `max` - the maximum delay, in milliseconds, between retries. Defaults to `30_000`.
  * `multiplier` - the factor the delay grows by on each successive attempt. Defaults to `2`.
  """

  @doc false
  def init(opts \\ []) do
    %{
      initial: Keyword.get(opts, :initial, 1_000),
      max: Keyword.get(opts, :max, 30_000),
      multiplier: Keyword.get(opts, :multiplier, 2)
    }
  end

  @doc false
  def next_delay(%{initial: initial, max: max, multiplier: multiplier} = state, attempt) do
    base = min(initial * :math.pow(multiplier, attempt - 1), max)

    delay = trunc(base / 2 + :rand.uniform() * base / 2)

    {delay, state}
  end
end

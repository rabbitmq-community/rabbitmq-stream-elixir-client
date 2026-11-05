defmodule RabbitMQStreamTest.ConnectionBackoff do
  use ExUnit.Case, async: true

  alias RabbitMQStream.Connection.Backoff
  alias RabbitMQStream.Connection.Backoff.Exponential

  @moduletag :v3_11
  @moduletag :v3_12
  @moduletag :v3_13
  @moduletag :v4_2
  @moduletag :v4_3

  test "default options" do
    assert Exponential.init([]) == %{initial: 1_000, max: 30_000, multiplier: 2}
  end

  test "delay grows with the attempt number, capped at :max" do
    state = Exponential.init(initial: 100, max: 400, multiplier: 2)

    assert {delay_1, _state} = Exponential.next_delay(state, 1)
    assert delay_1 in 50..100

    assert {delay_2, _state} = Exponential.next_delay(state, 2)
    assert delay_2 in 100..200

    assert {delay_3, _state} = Exponential.next_delay(state, 3)
    assert delay_3 in 200..400

    # Would be 800 uncapped -- clamped to :max instead.
    assert {delay_4, _state} = Exponential.next_delay(state, 4)
    assert delay_4 in 200..400
  end

  test "never gives up" do
    state = Exponential.init([])

    for attempt <- 1..20 do
      assert {_delay, _state} = Exponential.next_delay(state, attempt)
    end
  end

  test "keeps returning :max long after the exponential step would overflow a float" do
    state = Exponential.init(initial: 100, max: 400, multiplier: 2)

    assert {delay, _state} = Exponential.next_delay(state, 5_000)
    assert delay in 200..400
  end

  test "the shorthand-atom dispatcher resolves :exponential" do
    assert {Exponential, %{initial: 1_000}} = Backoff.init(:exponential)
  end

  test "the {shorthand, opts} dispatcher forwards opts to the resolved strategy" do
    assert {Exponential, %{initial: 50}} = Backoff.init({:exponential, initial: 50})
  end

  test "next_delay/2 dispatches through the resolved module and threads its state back" do
    backoff = Backoff.init({:exponential, initial: 10, max: 10})

    assert {delay, {Exponential, _state}} = Backoff.next_delay(backoff, 1)
    assert delay in 5..10
  end
end

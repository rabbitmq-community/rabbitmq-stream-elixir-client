defmodule RabbitMQStream.Connection.Transport do
  @moduledoc false
  @type t :: module()

  @callback connect(options :: Keyword.t()) :: {:ok, any()} | {:error, any()}
  @callback close(socket :: any()) :: :ok
  @callback send(socket :: any(), data :: iodata()) :: :ok | {:error, term()}
end

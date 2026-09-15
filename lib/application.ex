defmodule RabbitMQStream.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Registry, keys: :unique, name: RabbitMQStream.Connection.Pool.Registry},
      {DynamicSupervisor, strategy: :one_for_one, name: RabbitMQStream.Connection.Pool.DynamicSupervisor}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: RabbitMQStream.Supervisor)
  end
end

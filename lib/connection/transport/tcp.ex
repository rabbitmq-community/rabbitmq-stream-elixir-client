defmodule RabbitMQStream.Connection.Transport.TCP do
  @moduledoc false
  @behaviour RabbitMQStream.Connection.Transport

  def connect(options) do
    with {:ok, socket} <-
           :gen_tcp.connect(
             String.to_charlist(options[:host]),
             options[:port],
             [:binary, active: true],
             options[:connect_timeout]
           ),
         :ok <- :gen_tcp.controlling_process(socket, self()) do
      {:ok, socket}
    end
  end

  def close(socket) do
    :gen_tcp.close(socket)
  end

  def send(socket, data) do
    :gen_tcp.send(socket, data)
  end
end

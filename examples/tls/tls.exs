# TLS (mutual TLS) example: connects over the `:ssl` transport instead of plain `:tcp`.
#
# Requires a RabbitMQ node with the stream TLS listener enabled on port 5551 and the
# certificates under `services/cert`, e.g.:
#   docker compose -f services/docker-compose.yaml up -d rabbitmq_stream_4_3
#
# Run from the repo root with:
#   mix run examples/tls/tls.exs

stream_name = "tls-example-stream"

defmodule TLS.Connection do
  use RabbitMQStream.Connection,
    port: 5551,
    transport: :ssl,
    ssl_opts: [
      keyfile: "services/cert/client_box_key.pem",
      certfile: "services/cert/client_box_certificate.pem",
      cacertfile: "services/cert/ca_certificate.pem",
      verify: :verify_peer
    ]
end

defmodule TLS.Producer do
  use RabbitMQStream.Producer,
    connection: TLS.Connection,
    stream_name: stream_name
end

defmodule TLS.Consumer do
  use RabbitMQStream.Consumer,
    connection: TLS.Connection,
    stream_name: stream_name,
    initial_offset: :first

  @impl true
  def handle_message(message) do
    IO.puts("Received over TLS: #{message}")
    :ok
  end
end

{:ok, _connection} = TLS.Connection.start_link()

case TLS.Connection.create_stream(stream_name) do
  :ok -> :ok
  {:error, :stream_already_exists} -> :ok
end

{:ok, _producer} = TLS.Producer.start_link()
{:ok, _consumer} = TLS.Consumer.start_link()

for i <- 1..5 do
  TLS.Producer.publish("secure-message-#{i}")
end

Process.sleep(1_000)

TLS.Connection.delete_stream(stream_name)

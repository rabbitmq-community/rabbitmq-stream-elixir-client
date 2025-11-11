defmodule RabbitMQStream.Client do
  @moduledoc """
  Client for connection to a RabbitMQ Stream Cluster.

  This module provides the API for setting up and managing a connection to multiple RabbitMQ Stream
  nodes in a cluster. It implements RabbitMQStream.Connection.Behavior, but with added functionality
  related to managing multiple connections.


  ## Lifecycle

  At startup, the client creates a`RabbitMQStream.Connection` to the provided host. It is used to
  discover other nodes, mainly using the `query_metadata` command.


  """

  defmacro __using__(opts) do
    quote location: :keep do
      @opts unquote(opts)

      def child_spec(opts) do
        %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}
      end

      def start_link(opts \\ []) when is_list(opts) do
        opts =
          Application.get_env(:rabbitmq_stream, __MODULE__, [])
          |> Keyword.merge(@opts)
          |> Keyword.merge(opts)
          |> Keyword.put(:name, __MODULE__)

        RabbitMQStream.Client.start_link(opts)
      end

      def stop(reason \\ :normal, timeout \\ :infinity) do
        GenServer.stop(__MODULE__, reason, timeout)
      end
    end
  end

  def start_link(opts \\ []) when is_list(opts) do
    opts =
      Application.get_env(:rabbitmq_stream, :defaults, [])
      |> Keyword.get(:client, [])
      |> Keyword.merge(opts)

    GenServer.start_link(RabbitMQStream.Client.Lifecycle, opts, name: opts[:name])
  end

  def child_spec(opts) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}
  end

  defdelegate subscribe(server, stream_name, pid, offset, credit, properties \\ []), to: RabbitMQStream.Connection

  @type client_option ::
          {:auto_discovery, boolean()}
          | {:max_retries, non_neg_integer()}
          | {:proxied?, boolean()}

  defstruct [
    :max_retries,
    status: :setup,
    opts: [],
    control: nil,
    proxied?: false,
    monitors: %{},
    clients: %{},
    client_sequence: 0
  ]

  @type t :: %__MODULE__{
          control: pid() | nil,
          status: :open | :setup | :closed,
          client_sequence: non_neg_integer(),
          proxied?: boolean(),
          # Maps each subscriber to the connection's PID, so that we can garbage collect it when the subscriber dies
          monitors: %{
            reference() => {type :: :brooker | :subscriber | :producer, other :: reference(), id :: non_neg_integer()}
          },
          clients: %{
            reference() => {type :: :subscriber | :producer, subscriber :: pid(), connection :: pid(), args :: term()}
          },
          max_retries: non_neg_integer() | nil,
          opts: [RabbitMQStream.Connection.connection_options() | client_option()]
        }
end

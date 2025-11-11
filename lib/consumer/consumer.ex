defmodule RabbitMQStream.Consumer do
  @moduledoc """
  Used to declare a Persistent Consumer module. Then the `handle_message/1` callback will be invoked
  for each message received.

  # Usage

      defmodule MyApp.MyConsumer do
        use RabbitMQStream.Consumer,
          connection: MyApp.MyConnection,
          stream_name: "my_stream",
          initial_offset: :first

        @impl true
        def handle_message(_message) do
          # ...
          :ok
        end
      end


  # Parameters

  * `:connection` - The connection module to use. This is required.
  * `:stream_name` - The name of the stream to consume. This is required.
  * `:initial_offset` - The initial offset. This is required.
  * `:initial_credit` - The initial credit to request from the server. Defaults to `50_000`.
  * `:offset_tracking` - Offset tracking strategies to use. Defaults to `[count: [store_after: 50]]`.
  * `:flow_control` - Flow control strategy to use. Defaults to `[count: [credit_after: {:count, 1}]]`.
  * `:offset_reference` -
  * `:private` - Private data that can hold any value, and is passed to the `handle_message/2` callback.
  * `:serializer` - The module to use to decode the message. Defaults to `__MODULE__`,
    which means that the consumer will use the `decode!/1` callback to decode the message, which is
    implemented by default to return the message as is.


  * `:properties` - Define the properties of the subscription. Can only have one option at a time.
    * `:single_active_consumer`: set to `true` to enable [single active consumer](https://blog.rabbitmq.com/posts/2022/07/rabbitmq-3-11-feature-preview-single-active-consumer-for-streams/) for this subscription.
    * `:super_stream`: set to the name of the super stream the subscribed is a partition of.
    * `:filter`: List of strings that define the value of the filter_key to match.
    * `:match_unfiltered`: whether to return messages without any filter value or not.

  It is also possible to process chunks by implementing the `handle_chunk/1` or `handle_chunk/2` callbacks.

  # Offset Tracking

  The consumer is able to track its progress in the stream by storing its latests offset in the stream.
  Check [Offset Tracking with RabbitMQ Streams(https://blog.rabbitmq.com/posts/2021/09/rabbitmq-streams-offset-tracking/) for more information on how offset tracking works.

  The consumer can be configured to use different offset tracking strategies, which decide when to
  store the offset in the stream. You can implement your own strategy by implementing the
  `RabbitMQStream.Consumer.OffsetTracking` behaviour, and passing it to the `:offset_tracking` option.
  It defaults to `RabbitMQStream.Consumer.OffsetTracking.CountStrategy`, which stores the offset after,
  by default, every 50_000 messages.

  You can use the default strategies by passing a shorthand alias:

  * `interval` : `RabbitMQStream.Consumer.OffsetTracking.IntervalStrategy`
  * `count` : `RabbitMQStream.Consumer.OffsetTracking.CountStrategy`

      use RabbitMQStream.Consumer,
          connection: MyApp.MyConnection,
          stream_name: "my_stream",
          initial_offset: :first,
          offset_tracking: [
            count: [store_after: 50],
            interval: [store_after: 5_000]
          ]

  # Flow Control

  The RabbitMQ Streams server requires that the consumer declares how many messages it is able to
  process at a time. This is done by informing an amount of 'credits' to the server. After every chunk
  is sent, one credit is consumed, and the server will send messages only if there are credits available.


  We can configure the consumer to automatically request more credits based on a strategy.
  By default it uses the `RabbitMQStream.Consumer.FlowControl.MessageCount`, which
  requests 1 additional credit for every 1 processed chunk. Please check the
  RabbitMQStream.Consumer.FlowControl.MessageCount module for more information.


  You can also call `RabbitMQStream.Consumer.credit/2` to manually add more credits to the subscription,
  or implement your own strategy by implementing the `RabbitMQStream.Consumer.FlowControl` behaviour,
  and passing it to the `:flow_control` option.


  You can find more information on the [RabbitMQ Streams documentation](https://www.rabbitmq.com/stream.html#flow-control).


  If you want an external process to be fully in control of the flow control of a consumer, you can
  set the `:flow_control` option to `false`. Then you can call `RabbitMQStream.Consumer.credit/2` to
  manually add more credits to the subscription.

  # Configuration

  You can configure each consumer with:

      config :rabbitmq_stream, MyApp.MyConsumer,
        connection: MyApp.MyConnection,
        stream_name: "my_stream",
        initial_offset: :first,
        initial_credit: 50_000,
        offset_tracking: [count: [store_after: 50]],
        flow_control: [count: [credit_after: {:count, 1}]],
        serializer: Jason

  These options are overriden by the options passed to the `use` macro, which are overriden by the
  options passed to `start_link/1`.


  And also you can override the defaults of all consumers with:

        config :rabbitmq_stream, :defaults,
          consumer: [
            connection: MyApp.MyConnection,
            initial_credit: 50_000,
            # ...
          ],

  Globally configuring all consumers ignores the following options:

  * `:stream_name`
  * `:offset_reference`
  * `:private`

  # Decoding


  You can declare a function for decoding each message by declaring a `decode!/1` callback as follows:
      defmodule MyApp.MyConsumer do
        use RabbitMQStream.Consumer,
          connection: MyApp.MyConnection,
          stream_name: "my_stream",
          initial_offset: :first

        @impl true
        def decode!(message) do
          Jason.decode!(message)
        end
      end


  Or by passing a `:serializer` option to the `use` macro:
      defmodule MyApp.MyConsumer do
        use RabbitMQStream.Consumer,
          connection: MyApp.MyConnection,
          stream_name: "my_stream",
          initial_offset: :first,
          serializer: Jason
      end


  The default value for the `:serializer` is the module itself, unless a default is defined at a higher
  level of the configuration. If there is a `decode!/1` callback defined, it is always used


  # Properties
  You can provide additional properties to the consumer to change its behavior, by passing `:properties`.


  ## Single active consumer
  To use it, you must provide a "group_name". The server manages each consumer so the only one will
  of each group will be receiving chunks at a time.


  Although there is only one Consumer active, we must provide the server the offset a consumer starts
  on when being upgraded to the being the active one. To do so you must implement the `handle_update/2`
  callback, which must return a `{:ok, offset}` tuple.

      @impl true
      def handle_update(_, :upgrade) do
        {:ok, :last}
      end

      @impl true
      def handle_update(_, :downgrade) do
        # Must return something when being downgraded, but it is not used by the server.
        # Could be useful to use some external logic to persist the offset somewhere,
        #  so that it can be queried by the other consumer that is being upgraded
        {:ok, :last}
      end


  """
  require Logger

  defmacro __using__(opts) do
    defaults = Application.get_env(:rabbitmq_stream, :defaults, [])

    serializer = Keyword.get(opts, :serializer, Keyword.get(defaults, :serializer))

    quote location: :keep do
      @opts unquote(opts)
      require Logger

      @behaviour RabbitMQStream.Consumer.Behaviour

      def start_link(opts \\ []) do
        unless !Keyword.has_key?(opts, :serializer) do
          Logger.warning("You can only pass `:serializer` option to compile-time options.")
        end

        opts =
          Application.get_env(:rabbitmq_stream, __MODULE__, [])
          |> Keyword.merge(@opts)
          |> Keyword.merge(opts)
          |> Keyword.put_new(:consumer_module, __MODULE__)
          |> Keyword.put(:name, __MODULE__)

        RabbitMQStream.Consumer.start_link(opts)
      end

      def child_spec(opts) do
        %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}
      end

      def stop() do
        GenServer.stop(__MODULE__)
      end

      def credit(amount) do
        RabbitMQStream.Consumer.credit(__MODULE__, amount)
      end

      def get_credits() do
        RabbitMQStream.Consumer.get_credits(__MODULE__)
      end

      def store_offset() do
        RabbitMQStream.Consumer.store_offset(__MODULE__)
      end

      unquote(
        if serializer != nil do
          quote do
            def decode!(message), do: unquote(serializer).decode!(message)
          end
        else
          quote do
            def decode!(message), do: message
          end
        end
      )

      defoverridable RabbitMQStream.Consumer.Behaviour
    end
  end

  def start_link(opts \\ []) do
    opts =
      Application.get_env(:rabbitmq_stream, :defaults, [])
      |> Keyword.get(:consumer, [])
      |> Keyword.drop([:stream_name, :offset_reference, :private])
      |> Keyword.merge(opts)

    GenServer.start_link(RabbitMQStream.Consumer.LifeCycle, opts, name: opts[:name])
  end

  def child_spec(opts) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}
  end

  @spec credit(GenServer.server(), amount :: non_neg_integer()) :: :ok
  def credit(server, amount) do
    GenServer.cast(server, {:credit, amount})
  end

  @spec get_credits(GenServer.server()) :: non_neg_integer()
  def get_credits(server) do
    GenServer.call(server, :get_credits)
  end

  @spec store_offset(GenServer.server()) :: :ok
  def store_offset(server) do
    GenServer.call(server, :store_offset)
  end

  defstruct [
    :offset_reference,
    :connection,
    :stream_name,
    :offset_tracking,
    :flow_control,
    :id,
    # We could have delegated the tracking of the credit to the strategy,
    #  by adding declaring a callback similar to `after_chunk/3`. But it seems
    #  reasonable to have a `credit` function to manually add more credits,
    #  which would them possibly cause the strategy to not work as expected.
    :credits,
    :initial_credit,
    :private,
    :consumer_module,
    :initial_offset,
    last_offset: 0,
    properties: []
  ]

  @type t :: %__MODULE__{
          offset_reference: String.t(),
          connection: GenServer.server(),
          stream_name: String.t(),
          id: non_neg_integer() | nil,
          offset_tracking: [{RabbitMQStream.Consumer.OffsetTracking.t(), term()}],
          flow_control: {RabbitMQStream.Consumer.FlowControl.t(), term()},
          last_offset: non_neg_integer(),
          private: any(),
          credits: non_neg_integer(),
          initial_credit: non_neg_integer(),
          initial_offset: RabbitMQStream.Connection.offset(),
          properties: [RabbitMQStream.Message.Types.SubscribeRequestData.property()],
          consumer_module: module()
        }

  @type option ::
          {:offset_reference, String.t()}
          | {:connection, GenServer.server()}
          | {:stream_name, String.t()}
          | {:initial_offset, RabbitMQStream.Connection.offset()}
          | {:initial_credit, non_neg_integer()}
          | {:offset_tracking, [{RabbitMQStream.Consumer.OffsetTracking.t(), term()}]}
          | {:flow_control, {RabbitMQStream.Consumer.FlowControl.t(), term()}}
          | {:private, any()}
          | {:properties, [RabbitMQStream.Message.Types.SubscribeRequestData.property()]}

  @type opts :: [option()]
end

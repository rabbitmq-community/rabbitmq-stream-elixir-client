# Changelog

## Unreleased

### Breaking Changes

- **Automatic reconnection is now enabled by default.** A dropped TCP/TLS connection previously
  left `RabbitMQStream.Connection` in a broken state (`:open` with a dead socket) until the next
  write crashed the process; it now reconnects in place with backoff, and every producer/consumer
  attached to it is transparently redeclared/resubscribed once the connection reopens. Applications
  relying on the previous crash-then-supervisor-restart cycle (e.g. to reset external state) should
  pass `reconnect: false`, which now stops the connection in an orderly way instead
  (`{:shutdown, {:connection_closed, reason}}`) for a supervisor to restart, rather than crashing
  with a `MatchError` on the next write.
- New `:reconnect` (default `true`) and `:backoff` (default `:exponential`) connection options.
- New `:reconnecting` connection state.

## 0.4.2

Bug fixes

- Better handle the server's semver parsing #20

## 0.4.1

Bug fixes

- Split user and internal commands buffers

## 0.4.0

Added support for RabbitMQ 3.13, with Route, Partitions and SuperStreams support.

### 0.4.0 Features

- Support for `:consumerupdate`, `:exchangecommandversions`, `:streamstats`,  commands.
- Serialization options for encoding and decoding messages.
- TLS Support
- Functional `single-active-consumer`.
- Initial support for `:filter_value` consumer parameter, and `:createsuperstream`, `:deletesuperstream`, `:route`, `:partitions` commands.
- Initial support for SuperStreams, with RabbitMQStream.SuperConsumer and RabbitMQStream.SuperPublisher.
- Added `handle_message/1` callback for processing messages individually, ignoring messages happened before the minimal `:offset` exepected by the consumer, and the messages that don't match the `:filter_value`, as documented on the [blog post](https://blog.rabbitmq.com/posts/2023/10/stream-filtering/#on-the-consumer-side)

### 0.4.0 Changes

The 'Message' module tree was refactored to make all the Encoding and Decoding logic stay close to each other.

- Improved the cleanup logic for closing the connection.
- Publishers and Consumers now expects any name of a GenServer process, instead of a Module.
- Added checks on supported commands based on Server version, and exchanged commands versions.

### 0.4.0 Breaking Changes

- Renamed `RabbitMQStream.Subscriber` to `RabbitMQStream.Consumer`
- Renamed `RabbitMQStream.Publisher` to `RabbitMQStream.Producer`
- Each Deliver action made by the connection is now done with a `{:deliver, %RabbitMQStream.Message.Types.DeliverData{}} tuple
- Offset Tracking now prefers to use CommitedOffset when available
- The `handle_chunk/1` callback no longer receives decoded messages. It must be done manually by the user.

## 0.3.0

Added an implementation for a stream Consumer, fixed bugs and improved the documentation.

### 0.3.0 Features

- Added the `:credit` command.
- Added `RabbitMQStream.Subscriber`, which subscribes to a stream, while tracking its offset and credit based on customizeable strategies.
- Added the possibility of globally configuring the default Connection for Publishers and Subscribers

### 0.3.0 Bug Fixes

- Fixed an issue where tcp packages with multiple commands where not being correctly parsed, and in reversed order

### 0.3.0 Changes

- `RabbitMQStream.Publisher` no longer calls `connect` on the Connection during its setup.
- Moved `RabbitMQStream.Publisher`'s setup logic into `handle_continue`, to prevent locking up the application startup.
- `RabbitMQStream.Publisher` no longer declares the stream if it doesn't exists.
- `RabbitMQStream.Publisher` module now can optionally declare a `before_start/2` callback, which is called before it calls `declare_publisher/2`, and can be used to create the stream if it doesn't exists.
- `RabbitMQStream.Connection` now buffers the requests while the connection is not yet `:open`.

### 0.3.0 Breaking Changes

- Subscription deliver messages are now in the format `{:chunk, %RabbitMQ.OsirisChunk{}}`.

## 0.2.1

Documentation and Configuration refactoring

- It is now possible to define the connection and subscriber parameters throught the `config.exs` file
- Documentation improvements, and examples

## 0.2.0

The main objective of this release is to remove the manually added code from `rabbitmq_stream_common`'s Erlang implementation of Encoding and Decoding logic, with frame buffering.

## 0.1.0

Initial release with the following features:

- Opening connection to RabbitMQ server
- Declaring a Stream
- Creating a Stream Publisher
- Subscribing to Stream Messages
- Initial Hex Release

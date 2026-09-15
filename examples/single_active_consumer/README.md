# Single Active Consumer

Two consumers (`ConsumerA` and `ConsumerB`) join the same `single_active_consumer` group on
the same stream. RabbitMQ only ever delivers chunks to one member of the group at a time.

The example:

1. Starts `ConsumerA` first, which becomes the active consumer and receives the first batch
   of messages.
2. Starts `ConsumerB`, which joins the group but stays idle while `ConsumerA` is active.
3. Stops `ConsumerA`, which causes the server to promote `ConsumerB` to active. `ConsumerB`
   then receives the second batch of messages.

Both consumers implement the `handle_update/2` callback, which the server calls whenever a
consumer is upgraded to active or downgraded to inactive, so it can decide where to resume
consuming from.

Run with:

```bash
mix run examples/single_active_consumer/single_active_consumer.exs
```

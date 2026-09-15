# Super Stream

Declares a Super Stream (RabbitMQ 3.13+) with two partitions, `eu` and `latam`, then uses
`RabbitMQStream.SuperProducer` and `RabbitMQStream.SuperConsumer` to publish/consume across
both. The producer routes each message to a partition via a custom `routing_key/2` callback.

## Single Active Consumer

A single `SuperConsumer` instance never actually exercises single-active-consumer — with
nothing competing for a partition, its per-partition consumer is trivially "the only, hence
active, one". The real point only shows up once you run *multiple instances* of the consuming
application against the same super stream (e.g. horizontally scaled replicas). This example
simulates that with two independent `SuperConsumer` modules, each with its own connection.

Two things worth watching for in the output:

1. **RabbitMQ rebalances on join, not just on disconnect.** The moment instance B starts, the
   server immediately hands it one partition (instance A keeps the other), to keep partitions
   spread evenly across instances — it doesn't wait for anything to fail first.
2. **Simulating instance A going away means stopping both its `SuperConsumer` and its
   `Connection`.** That combination is what triggers the server to hand its partition over to
   instance B, and it mirrors a real deployment: a crashed or shut-down instance takes its whole
   connection with it, not just one subscription. `handle_update(:upgrade)` then queries the last
   stored offset so instance B resumes without replaying what instance A already processed — the
   example uses a low `offset_tracking: [count: [store_after: 2]]` threshold so an offset is
   actually available by the time instance A goes away.

Run with:

```bash
mix run examples/super_stream/super_stream.exs
```

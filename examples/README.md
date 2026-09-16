# Stream examples

Each subfolder is a self-contained, runnable script demonstrating one feature of the client.
They all expect a RabbitMQ node with the stream plugin enabled on `localhost:5552` (run
`docker compose -f services/docker-compose.yaml up -d rabbitmq_stream_4_3` from the repo root
to start one).

Run any example from the repo root with:

```bash
mix run examples/<folder>/<file>.exs
```

- [Getting started](./getting_started/getting_started.exs) - Producer and Consumer, the structures you need to start.
- [Offset Start](./offset_start/offset_start.exs) - How to set different points to start consuming from.
- [Offset Tracking](./offset_tracking/offset_tracking.exs) - Manually store the consumer's offset.
- [Automatic Offset Tracking](./automatic_offset_tracking/automatic_offset_tracking.exs) - Automatically store the consumer's offset via the count strategy, and resume from it with a new consumer.
- [Getting started TLS](./tls/tls.exs) - A TLS/mTLS example (run `docker compose -f services/docker-compose.yaml up -d rabbitmq_stream_4_3` to get a TLS-enabled node on port 5551).
- [Stream Filtering](./filtering/filtering.exs) - Stream filtering example (RabbitMQ 3.13+).
- [Single Active Consumer](./single_active_consumer) - Single Active Consumer example.
- [Super Stream](./super_stream) - Super Stream example with Single Active Consumer.
- [Manual Credit](./manual_credit/manual_credit.exs) - Manual credit/flow-control strategy example.
- [Deduplication](./deduplication/deduplication.exs) - Message deduplication via a stable producer `reference_name`.

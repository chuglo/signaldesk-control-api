# signaldesk-control-api

**Not for production use.**

SignalDesk control-plane API and transactional outbox publisher.

## Outbox publisher

The publisher is a separate process built from the same package/image as the API. A
future Docker Compose service should override the image command with:

```text
signaldesk-outbox-publisher
```

Use `signaldesk-outbox-publisher --once` for a single bounded batch. The default
mode polls continuously with a bounded, stop-event-aware wait. `SIGTERM` sets that
event so supervisors can stop polling and reach Redis/engine cleanup promptly;
`KeyboardInterrupt` remains supported. Both modes require only
`SIGNALDESK_DATABASE_URL` and `SIGNALDESK_REDIS_URL`. The publisher's dedicated
settings model deliberately has no BFF or worker credentials, so its container
does not receive unrelated service identities.

Every outbox payload is parsed by the frozen `signaldesk-contracts` package before
Redis. The publisher writes only canonical event JSON and its event ID. An atomic
Redis Lua operation creates the stream entry and a durable, non-secret marker key
for the outbox event together. On retry, a marker is accepted only after Lua
verifies that it names the expected stream and that the exact stream ID still
contains the same canonical event JSON and event UUID. A malformed/forged marker,
a deleted or trimmed entry, or any stream, ID, or payload mismatch fails closed and
leaves PostgreSQL unpublished; the publisher neither deletes the marker nor issues
a replacement `XADD`. A legitimate retry after PostgreSQL commit failure can
therefore mark the row without another `XADD`. Workers must still provide
at-least-once/idempotent processing semantics.

Each bounded call attempts rows in deterministic order, one row and one PostgreSQL
transaction at a time with `FOR UPDATE SKIP LOCKED`. A failed row remains retryable
but is excluded from the rest of that call, so it cannot roll back an earlier mark
or starve later rows in the same bounded batch.

The publisher supports **standalone Redis only**. It checks Redis cluster metadata
before publication and rejects `cluster_enabled=1` before `EVAL`, `XADD`, or a
database mark. The required explicit stream names intentionally do not use Redis
Cluster hash tags. Corpus Compose must therefore provision a standalone Redis
instance, not Redis Cluster.

Development uses uv's portable sibling source declaration
`../signaldesk-contracts`. A wheel contains the normal dependency
`signaldesk-contracts==0.2.0`, not the local path. The later Compose image build
must therefore include both sibling repositories in its build context and install
the contracts wheel first (or resolve version 0.2.0 from a package index); copying
contracts into this repository would break the independent repository boundary.

## License

MIT. See [LICENSE](LICENSE).

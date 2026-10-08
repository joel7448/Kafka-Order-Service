# Kafka Order Processing with Retry and DLQ

This project demonstrates a simple Kafka-based order processing pipeline with three stages:

- a producer that emits order events
- a main consumer that processes orders
- a retry consumer that reprocesses transient failures
- a dead-letter queue (DLQ) consumer that captures terminal failures

It is designed as a practical sample for learning Kafka messaging patterns, retry backoff, and failure isolation in distributed systems.

## Architecture overview

The application uses these Kafka topics:

- `orders` — the main stream of incoming order events
- `orders-retry` — failed messages that are retried with backoff
- `orders-dlq` — failed messages that exceed retry limits

The processing flow works like this:

1. The producer publishes sample order messages to the `orders` topic.
2. The main worker reads messages from `orders` and calls `processOrder()`.
3. If processing fails with a transient error, the message is re-routed to `orders-retry`.
4. The retry worker waits with exponential backoff, re-attempts processing, and either:
   - succeeds and exits the retry loop, or
   - requeues until the max retry threshold is reached.
5. Once the retry limit is hit, the message is sent to `orders-dlq`.
6. The DLQ worker logs the failure details for monitoring, alerting, or downstream analysis.
7. A manual replay CLI can re-publish a DLQ message back to the main topic for investigation or recovery.

## Project structure

```text
.
├── docker-compose.yaml
├── package.json
├── README.md
├── src/
│   ├── cli/
│   │   └── replayDlq.js
│   ├── config/
│   │   ├── constants.js
│   │   └── kafka.js
│   ├── producer/
│   │   └── publisher.js
│   ├── services/
│   │   └── orderService.js
│   ├── utils/
│   │   └── headers.js
│   └── workers/
│       ├── dlqWorker.js
│       ├── mainWorker.js
│       └── retryWorker.js
```

## Sample behavior

The producer sends four example orders:

- `NORMAL` — succeeds immediately
- `TRANSIENT_FLAKE` — fails randomly with a temporary payment timeout
- `POISON_PILL` — fails as a fatal invalid payload / schema problem
- another `TRANSIENT_FLAKE` message — exercises the retry flow again

This makes it easy to observe:

- successful processing
- retries with delay
- final DLQ routing for unrecoverable messages

## Prerequisites

Before running the project, install:

- Node.js 18+
- Docker and Docker Compose
- npm

## Quick start

1. Start Kafka and Kafka UI:

```bash
docker compose up -d
```

This starts:

- Kafka broker on `localhost:9092`
- Kafka UI on `http://localhost:8080`

2. Install dependencies:

```bash
npm install
```

3. Start the main order processor:

```bash
npm run worker:main
```

4. Start the retry processor in a second terminal:

```bash
npm run worker:retry
```

5. Start the DLQ monitor in a third terminal:

```bash
npm run worker:dlq
```

6. Publish sample events:

```bash
npm run produce
```

## Available scripts

```bash
npm run produce
npm run worker:main
npm run worker:retry
npm run worker:dlq
npm run dlq:replay
```

### Script meanings

- `produce`: publishes demo order messages to Kafka
- `worker:main`: consumes from `orders`
- `worker:retry`: consumes from `orders-retry` with backoff retry logic
- `worker:dlq`: listens on `orders-dlq` and logs dead-letter records
- `dlq:replay`: interactive CLI that allows selecting DLQ records for replay to the main topic

## Replaying messages from the DLQ

To inspect and manually replay dead-lettered events:

```bash
npm run dlq:replay
```

The CLI presents each DLQ message and lets you choose:

- `r` — replay the message back to the main `orders` topic
- `s` — skip and discard it
- `q` — quit the session

This is useful for operational recovery when a message is not truly bad, but needs another pass through the normal processing path.

## Configuration

The project uses environment variables defined in code via defaults:

- `KAFKA_CLIENT_ID` — Kafka client ID; defaults to `order-system`
- `KAFKA_BROKERS` — Kafka broker list; defaults to `localhost:9092`

Example:

```bash
export KAFKA_BROKERS=localhost:9092
export KAFKA_CLIENT_ID=my-order-system
```

## Retry behavior

The retry worker uses exponential backoff:

- attempt 1 → 2s delay
- attempt 2 → 4s delay
- attempt 3 → 8s delay

The retry limit is defined in `src/config/constants.js` as `MAX_ATTEMPTS: 3`.

## Notes

This repository is intentionally lightweight and educational. It does not include production-grade persistence, schema registry, or full orchestration. Instead, it focuses on the core Kafka design patterns:

- event-driven processing
- idempotent message handling patterns
- retry and DLQ workflows
- operational replay tools

## Example flow summary

```text
Producer -> orders -> Main Worker -> (success)
                              \-> (failure) -> orders-retry -> Retry Worker
                                                            \-> (max retries) -> orders-dlq
```

## License

This project is for learning and demonstration purposes.

# Webhook Ingestion Service
![Local version](./diagrams/webhook_ingestion_service_(local).png)
A webhook ingestion engine for payment processor events. Trying to push
Go to 500K+ req/s — a step up from my previous fintech projects
([pgpockets](https://github.com/whotterre/pgpockets)).


## What I'm exploring

Concepts from:
- *Cloud Native Go* — Matthew Titmus
- *Concurrency in Go* — Catherine Cox-Buday
- Maybe a few from Michael Nygard's *Release It! Design and Deploy Production-Ready Software*
- ([go-kata](https://github.com/MedUnes/go-kata))


Specifically:
- Durable buffering before ack (Kafka)
- Per-key ordering with horizontal consumers (RabbitMQ modulus-hash)
- Idempotency on top of at-least-once delivery
- Backpressure and graceful degradation
- Chaos testing — duplicate, reorder, kill mid-request, replay

## Stack

Go 1.26+ · Postgres 16 · Apache Kafka · RabbitMQ · Redis

The local branch would serve as a form of scratch pad for this..

---

## Status Update

### What Has Been Done

1. **Ingest API Gateway (`src/cmd/ingest`)**
   - Implemented HTTP webhook server (`POST /webhook`) verifying PSP payloads with HMAC-SHA256 (`X-PSP-Signature: t=...,v1=...`).
   - Routes payloads to an Apache Kafka partitioned topic (`raw.events`), keyed by `account_id` to maintain strict per-account event ordering.

2. **Dispatcher Tier (`src/cmd/dispatch`)**
   - Consumes raw events from Kafka via Sarama consumer groups.
   - Dual-path routing:
     - **Audit Archive**: Compresses incoming payloads to local daily archives (`archives/raw_events_YYYY-MM-DD.json.gz`) for compliance and offline replay.
     - **RabbitMQ Fan-Out**: Distributes events deterministically across 64 queues (`webhooks-0` to `webhooks-63`) using modulus hashing on `account_id`.

3. **Two-Layer Deduplication & Ledger Engine (`src/internal/ledger`)**
   - **L1 In-Memory Rejection**: Redis Bloom filter module (`CheckAndMark`) filtering out duplicate retries in microseconds.
   - **L2 ACID State Store**: PostgreSQL append-only ledger (`ledger_entries`) executed inside transactions with row locks on `idempotency_keys` (`SELECT ... FOR UPDATE`).
   - Handles credit/debit deltas based on event types (`payment.succeeded`, `payment.refunded`, `chargeback.created`).

4. **Account & Dynamic Checkout Service (`src/cmd/account`)**
   - Distributed lock-protected 10-digit virtual account generator (`6930...`) in Redis.
   - User authentication and onboarding.
   - 15-minute dynamic checkout session management (`POST /api/v1/checkout/initiate`, `GET /api/v1/checkout/status`).
   - Automatic reconciliation: When a payment event arrives for an account, the active pending checkout session transitions to `PAID`.

5. **Worker Tier (`src/cmd/worker`)**
   - Concurrent consumer pool handling partitioned RabbitMQ queues:
     - **Ledger Worker**: Deduplicates and writes financial entries to PostgreSQL.
     - **Notification Worker**: Renders HTML receipts and delivers transactional emails via Brevo (Sendinblue) API.
     - **Analytics Worker**: Aggregates throughput and latency metrics for Prometheus.

6. **Operational Replay Tooling (`src/cmd/replay`)**
   - CLI utility to drain poison messages from RabbitMQ Dead-Letter Queues (`dlq.webhooks`) and re-inject them into the pipeline.
   - Archive replayer streaming historical `.json.gz` dumps back into RabbitMQ.

7. **Stress Testing & Observability**
   - Grafana k6 stress-test script (`src/k6/load.js`) ramping up to 150 concurrent VUs with ~10% duplicate webhook retries.
   - Prometheus metrics exports (`/metrics`) across Ingest, Dispatcher, and Workers.

---

### Issues Faced & How They Were Fixed

#### 1. RabbitMQ Exchange Mismatch (Events Not Reaching Workers)
- **Issue**: Ingest and Dispatcher were successfully processing webhooks from Kafka, but worker queues remained completely empty (`webhooks-0` .. `webhooks-63`).
- **Root Cause**: The default exchange name in `config.go` was set to `"webhooks-0"` instead of `"webhooks"`. The publisher was declaring and publishing to `"webhooks-0"`, while the worker pool was listening to queues bound to `"webhooks"`.
- **Fix**: Corrected `RabbitMQExchange` default in `src/config/config.go` to `"webhooks"`, ensuring all 64 queues received routed messages.

#### 2. CloudAMQP Exchange Declaration Precondition Failures (406 Channel Error)
- **Issue**: Workers crashed on startup with `PRECONDITION_FAILED - inequivalent arg 'type' for exchange 'webhooks': received 'topic' but current is 'direct'`.
- **Root Cause**: The managed CloudAMQP instance already had `webhooks` created as a direct exchange. Trying to run `ExchangeDeclare` with type `topic` caused RabbitMQ to terminate the AMQP channel.
- **Fix**: Replaced blind `ExchangeDeclare` calls with `ExchangeDeclarePassive` first to verify existing exchange topology before attempting creation.

#### 3. High Ingest Tail Latency (p95 > 1.5s under 150 VUs)
- **Issue**: k6 load test reported a p95 latency of 1.06s – 1.55s on `POST /webhook`, failing the `p(95) < 1000ms` threshold.
- **Root Cause**: Ingest was using `sarama.SyncProducer` with `WaitForAll` (`acks=all`) over the public internet to an Aiven Cloud Kafka cluster. Each HTTP handler blocked waiting for a multi-broker TLS round-trip across the WAN.
- **Fix**: Switched Kafka producer acknowledgment setting to `sarama.WaitForLocal` (`acks=1`), allowing the leader broker to acknowledge immediately without waiting for cross-WAN replica round-trips.

#### 4. Email Template Parsing Failure in Notifier Worker
- **Issue**: Notifier worker logged `warning: could not initialize email notifier with template "...": failed to parse email template: template: reciept.html:29: function "formatAmount" not defined. Running in log-only mode.`
- **Root Cause**: The receipt HTML template invoked an unregistered Go template helper `{{ formatAmount .Amount }}`.
- **Fix**: Updated `reciept.html` to directly render `{{ .Currency }} {{ .Amount }}`, allowing standard Go `html/template.ParseFiles` to parse and initialize the email engine cleanly.

#### 5. Brevo API 401 Unauthorized
- **Issue**: Notification worker threw `Brevo API returned 401: {"message":"Key not found","code":"unauthorized"}` when attempting to send payment receipts.
- **Root Cause**: The environment variable was configured with an SMTP password or invalid key instead of a Brevo v3 REST API key.
- **Fix**: Generated a new v3 API key (`xkeysib-...`) from the Brevo dashboard and verified `BREVO_FROM_EMAIL` matched a verified sender address.

---

## Benchmark & Load Testing History

Performance benchmarks were executed using Grafana k6 (`src/k6/load.js`) simulating ramping concurrent virtual users (up to 150 VUs) with ~10% duplicate webhook retries over a 1m10s duration:

### Iteration 1: Synchronous `WaitForAll` over Public Internet (Baseline)
- **Producer Config**: `sarama.WaitForAll` (`acks=all`), synchronous `SyncProducer`
- **Results**:
  - **Total Requests**: 8,166 requests (~115.9 req/s)
  - **Error Rate**: `0.00%` (0 failed requests out of 8,166)
  - **Latency Profile**:
    - `min`: 246.80ms
    - `med`: 629.87ms
    - `p(90)`: 945.97ms
    - `p(95)`: **1.06s** (threshold: `< 1000ms` ✗)
    - `max`: 1.44s
- **Takeaway**: 100% durable and zero drops under load, but tail latency exceeded 1 second due to waiting for full multi-replica quorum round-trips over WAN to Aiven Cloud Kafka.

### Iteration 2: Peak Concurrency Stress (150 VUs under Heavy Network Contention)
- **Producer Config**: `sarama.WaitForAll` (`acks=all`)
- **Results**:
  - **Total Requests**: 7,004 requests (~99.5 req/s)
  - **Error Rate**: `0.00%` (0 failed requests out of 7,004)
  - **Latency Profile**:
    - `min`: 241.70ms
    - `med`: 741.55ms
    - `p(90)`: 1.25s
    - `p(95)`: **1.33s** (threshold: `< 1000ms` ✗)
    - `max`: 1.60s
- **Takeaway**: TCP connection multiplexing over WAN became the clear bottleneck under 150 concurrent goroutines waiting on synchronous socket round-trips.

### Iteration 3: Switched to `WaitForLocal` Leader Acknowledgments
- **Producer Config**: `sarama.WaitForLocal` (`acks=1`), committing in `4fc66d7`
- **Results**:
  - **Total Requests**: 5,592 requests (~78.3 req/s under fluctuating network conditions)
  - **Error Rate**: `0.00%` (0 failed requests out of 5,592)
  - **Latency Profile**:
    - `min`: 316.66ms
    - `med`: 936.54ms
    - `p(90)`: 1.30s
    - `p(95)`: **1.55s**
    - `max`: 4.94s
- **Takeaway**: Confirmed that when testing across public internet from a local environment to cloud-hosted brokers, network RTT (baseline 250ms–320ms) sets the lower floor. For true 500k+ req/s and single-digit millisecond p95s, asynchronous micro-batching (`sarama.AsyncProducer`) and co-located VPC networking are required.

---

## Changelog

### [2026-11-10]
- `df8c6a8` - **chore(email)**: add email template omitted in previous email integration for notification worker
- `b14d6dd` - **chore**: cosmetic changes and addition of gitignore file
- `ce40585` - **test(webhook)**: introduce ramping stress test scenario and retry simulation
- `4fc66d7` - **fix**: adjust Kafka producer to use WaitForLocal acknowledgment for lower latency
- `3897e6c` - **feat**: implement notifier worker (uses Brevo to send mail)

### [2026-11-09]
- `fda6731` - **deps**: add necessary deps for Postgres pg driver, Prometheus, and Redis clients
- `60b1c60` - **refactor**: cosmetic refactor for calc to get remaining time for checkout session to expire and change of bank name to "Bankee"
- `a4df392` - **feat**: add archive module to store event logs for replay
- `b28b22d` - **docs**: refine architecture to include circuit breakers, and timed checkout for webhook payment sessions
- `1faa5ad` - **feat**: add Redis Bloom filter deduplicator module
- `9ed3b01` - **feat**: add payment reconciliation for dynamic checkout accounts
- `957ba3c` - **feat**: add 15-minute dynamic checkout session management
- `580ae3c` - **feat**: add user authentication and onboarding service
- `4b76719` - **feat**: add distributed lock and 10-digit virtual account generator

### [2026-11-07]
- `531affe` - **chore(docker)**: add Prometheus, Grafana dashboards, and Postgres init configs
- `cf95111` - **feat**: add logic declare RabbitMQ publisher and logic to declare necessary RabbitMQ primitives on startup
- `e95cbf7` - **feat**: add dispatch module to consume events from Kafka and route to RabbitMQ queues

### [2026-11-04 - 2026-10-07]
- `f965cc9` - **feat**: add metrics package for Prometheus monitoring
- `78bf291` - **feat**: add config params for dispatch module
- `4d3dd7d` - **feat**: adjust Kafka producer with SCRAM config
- `4a5639f` - **feat**: add initialization logic to declare queues (worker queues and DLQ) and exchanges
- `b24e1ec` - **feat**: add consumer module for RabbitMQ
- `59bbb54` - **feat**: add dispatch module to move messages from Kafka to RabbitMQ
- `2c2b46b` - **refactor**: move TLS config logic in Kafka producer module to separate function
- `b5299bb` - **feat**: add RabbitMQ publisher logic

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

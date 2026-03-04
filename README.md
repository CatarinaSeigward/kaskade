# Kaskade

Real-time market analytics engine that ingests **10K+ events/sec**, computes streaming aggregations, and detects anomalies with sub-second latency.

```
Simulator ──> Kafka (3 brokers, Avro) ──> Spark Structured Streaming ──> Redis + PostgreSQL ──> Grafana
                                                    │
                                              Anomaly Detection ──> Alerts Topic ──> Alert Consumer
```

## Tech Stack

**Kafka 3.x** (KRaft) | **Confluent Schema Registry** | **Spark 3.5** (Scala) | **Redis 7** | **PostgreSQL 16** | **Grafana 10** | **FastAPI** | **Docker Compose**

## Quick Start

```bash
cp .env.example .env
docker compose up -d
```

| Service | URL |
|---------|-----|
| Grafana | [localhost:3000](http://localhost:3000) (admin/admin) |
| REST API | [localhost:8000/docs](http://localhost:8000/docs) |
| Kafka UI | [localhost:8080](http://localhost:8080) |

## What It Does

- **Ingestion** -- Python producer generates realistic tick data (GBM + jump diffusion), serializes with Avro, publishes to Kafka at 10K+ msg/sec with exactly-once semantics
- **Processing** -- Spark Structured Streaming performs 1-min sliding window aggregations (VWAP, volatility, RSI) with 30s watermark for late data
- **Detection** -- Z-score anomaly detection on price/volume/volatility with severity classification (INFO/WARNING/CRITICAL) and Redis-backed rolling statistics
- **Serving** -- Redis cache for real-time dashboard reads, PostgreSQL for historical queries, FastAPI for programmatic access
- **Alerting** -- Kafka-driven alert pipeline with Redis SET NX deduplication and Grafana webhook annotations
- **Dashboards** -- 3 auto-provisioned Grafana dashboards: Market Overview, Symbol Deep Dive, Operational Health

## API

```
GET /api/v1/metrics/{symbol}/latest    # real-time from Redis
GET /api/v1/metrics/{symbol}/history   # historical from PostgreSQL
GET /api/v1/metrics/topmovers          # top movers by price change
GET /api/v1/alerts                     # filtered alert history
GET /api/v1/health                     # service connectivity check
```

## Project Layout

```
producer/           Python Kafka producer + GBM simulator
spark-streaming/    Scala Spark app (window agg + anomaly detection + Redis/PG sinks)
alert-consumer/     Python alert pipeline with dedup
api/                FastAPI REST API
schemas/            Avro schemas (raw_trade, enriched_event, alert)
grafana/            Auto-provisioned dashboards and datasources
scripts/            Topic creation, DB init, startup
tests/              Integration + performance benchmarks
```

## Testing

```bash
pytest producer/tests/ -v                # simulator unit tests
cd spark-streaming && sbt test           # Scala unit tests
pytest tests/integration/ -v             # end-to-end (requires docker compose up)
python -m tests.performance.benchmark    # throughput + latency benchmark
```

# Kaskade

**Real-time market analytics engine built on Kafka, Spark Structured Streaming, Redis, and Grafana.**

A production-grade streaming platform that ingests 10K+ market events/sec, performs windowed aggregations (VWAP, volatility, RSI) and anomaly detection via Z-score analysis, and delivers sub-second alerts and live dashboards for market surveillance.

## Architecture

```
Market Data Sources (API / WebSocket / Simulator)
        |
Kafka Producer (Schema Registry + Avro Serialization)
        |
Kafka Cluster (3 brokers, KRaft mode)
  Topics: raw-trades | enriched-events | alerts
        |                              |
Spark Structured Streaming     Alert Consumer
  - Window Aggregation           - Deduplication (Redis)
  - Anomaly Detection            - PostgreSQL Audit Log
  - Feature Enrichment           - Grafana Webhooks
        |
Redis (Real-Time Cache) + PostgreSQL (Persistent Store)
        |
Grafana Dashboards + FastAPI REST API
```

## Technology Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Message Broker | Apache Kafka 3.x (KRaft) | Event streaming with 3-broker cluster |
| Schema Management | Confluent Schema Registry | Avro schema evolution (BACKWARD compat) |
| Stream Processing | Spark Structured Streaming 3.5 | Windowed aggregations & anomaly detection |
| State Cache | Redis 7.x | Sub-ms real-time dashboard reads |
| Persistence | PostgreSQL 16 | Historical analytics & alert audit log |
| Visualization | Grafana 10.x | Real-time & historical dashboards |
| API Layer | FastAPI | REST API with auto-generated OpenAPI docs |
| Deployment | Docker Compose | Full orchestration of 10+ services |
| CI/CD | GitHub Actions | Automated lint, test, build pipeline |

## Quick Start

```bash
# 1. Clone and navigate to project
git clone https://github.com/<your-username>/kaskade.git
cd kaskade

# 2. Copy environment config
cp .env.example .env

# 3. Start all services
bash scripts/start.sh

# Or manually with Docker Compose:
docker compose up -d
```

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Kafka UI | http://localhost:8080 | - |
| Grafana | http://localhost:3000 | admin / admin |
| REST API | http://localhost:8000 | - |
| API Docs (Swagger) | http://localhost:8000/docs | - |
| Spark Master UI | http://localhost:8082 | - |
| Schema Registry | http://localhost:8081 | - |

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/metrics/{symbol}/latest` | Latest real-time metrics from Redis |
| GET | `/api/v1/metrics/{symbol}/history` | Historical metrics with time-range filter |
| GET | `/api/v1/metrics/topmovers` | Top N symbols by price change |
| GET | `/api/v1/alerts` | Recent alerts with severity/symbol filters |
| GET | `/api/v1/alerts/summary` | Alert counts by symbol and severity |
| GET | `/api/v1/health` | Service health (Kafka, Redis, PostgreSQL) |

## Project Structure

```
.
├── docker-compose.yml          # Full service topology (10+ containers)
├── schemas/                    # Avro schema definitions
│   ├── raw_trade.avsc          # Raw trade event schema
│   ├── enriched_event.avsc     # Windowed aggregation schema
│   └── alert.avsc              # Anomaly alert schema
├── producer/                   # Kafka producer application (Python)
│   ├── src/
│   │   ├── main.py             # Entry point with topic creation
│   │   ├── market_simulator.py # GBM + Jump Diffusion simulator
│   │   ├── market_data_fetcher.py # Live API data source
│   │   ├── avro_serializer.py  # Schema Registry integration
│   │   └── kafka_producer.py   # High-throughput producer wrapper
│   └── config/settings.py      # Configuration management
├── spark-streaming/            # Spark application (Scala)
│   ├── src/main/scala/com/kaskade/
│   │   ├── StreamingApp.scala      # Main driver with 4 streaming queries
│   │   ├── WindowAggregation.scala # VWAP, volatility, RSI computation
│   │   ├── AnomalyDetection.scala  # Z-score anomaly detection
│   │   ├── RedisSink.scala         # Redis state updater
│   │   └── PostgresSink.scala      # PostgreSQL persistence
│   └── build.sbt
├── alert-consumer/             # Alert pipeline (Python)
│   └── src/
│       ├── consumer.py         # Kafka consumer with dedup
│       └── main.py             # Entry point
├── api/                        # FastAPI REST API (Python)
│   └── src/
│       ├── main.py             # FastAPI app
│       ├── models.py           # Pydantic response models
│       ├── dependencies.py     # Redis/PostgreSQL connection pools
│       └── routes/             # API route handlers
├── grafana/                    # Dashboard provisioning
│   └── provisioning/
│       ├── datasources/        # Auto-configured data sources
│       └── dashboards/         # 3 production dashboards (JSON)
├── scripts/                    # Operational scripts
├── tests/                      # Integration & performance tests
└── .github/workflows/ci.yml    # CI/CD pipeline
```

## Key Features

### Streaming Pipeline
- **10K+ events/sec** ingestion via multi-partition Kafka topics
- **Exactly-once semantics** with idempotent producer and Spark checkpointing
- **Sliding window aggregations** (1-min window, 30-sec slide) with watermark-based late data handling
- **Schema evolution** via Confluent Schema Registry (BACKWARD compatibility)

### Anomaly Detection
- **Multi-signal detection**: Price Z-score, volume spike, volatility spike
- **Severity classification**: INFO (Z>2), WARNING (Z>3), CRITICAL (Z>4)
- **Redis-backed rolling statistics** for cross-query state sharing
- **Deduplication** via Redis SET NX with 5-minute TTL

### Market Simulator
- **Geometric Brownian Motion** with Poisson jump diffusion
- Configurable throughput (1K-50K events/sec)
- 10 realistic ticker symbols with correlated price dynamics

### Observability
- **3 Grafana dashboards**: Market Overview, Symbol Deep Dive, Operational
- **Pipeline health monitoring**: Processing lag, data freshness, alert rates
- **Auto-provisioned** data sources and dashboards

## Testing

```bash
# Unit tests (Python)
pytest producer/tests/ -v
pytest api/tests/ -v

# Unit tests (Scala)
cd spark-streaming && sbt test

# Integration tests (requires Docker Compose services)
docker compose up -d kafka-1 kafka-2 kafka-3 schema-registry redis postgres
pytest tests/integration/ -v

# Performance benchmark
python -m tests.performance.benchmark --duration 10
```

## Performance Targets

| Metric | Target | Measurement |
|--------|--------|-------------|
| Ingestion Throughput | 10,000+ events/sec | Kafka producer metrics |
| End-to-End Latency | p50 < 500ms, p99 < 1s | Embedded timestamps |
| Window Accuracy | < 0.01% deviation | Batch comparison |
| Alert Precision | > 95% true positive | Synthetic anomalies |

"""
Producer configuration settings.

Loads configuration from environment variables with sensible defaults
for local development. All Kafka producer tuning parameters follow
the project plan specifications.
"""

import os
import json


class Settings:
    """Centralized configuration for the Kafka producer application."""

    # Kafka broker connection
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"
    )

    # Schema Registry endpoint
    SCHEMA_REGISTRY_URL: str = os.getenv(
        "SCHEMA_REGISTRY_URL", "http://localhost:8081"
    )

    # Kafka topic for raw trade events
    RAW_TRADES_TOPIC: str = os.getenv("RAW_TRADES_TOPIC", "raw-trades")

    # Producer tuning (per project plan spec)
    # acks=all: Wait for all in-sync replicas to acknowledge
    PRODUCER_ACKS: str = "all"
    # Enable idempotent producer for exactly-once semantics
    PRODUCER_ENABLE_IDEMPOTENCE: bool = True
    # Max in-flight requests (must be <= 5 for idempotent producer)
    PRODUCER_MAX_IN_FLIGHT: int = 5
    # Batching: linger 5ms to accumulate micro-batches
    PRODUCER_LINGER_MS: int = 5
    # Batch size: 16KB per partition batch
    PRODUCER_BATCH_SIZE: int = 16384
    # Compression: Snappy for low-latency compression
    PRODUCER_COMPRESSION_TYPE: str = "snappy"
    # Buffer memory: 32MB total producer buffer
    PRODUCER_BUFFER_MEMORY: int = 33554432

    # Producer mode: "simulator" or "live"
    PRODUCER_MODE: str = os.getenv("PRODUCER_MODE", "simulator")

    # Simulator configuration
    SIMULATOR_SYMBOLS: list = os.getenv(
        "SIMULATOR_SYMBOLS",
        "AAPL,GOOGL,MSFT,AMZN,TSLA,META,NVDA,JPM,BAC,GS"
    ).split(",")

    SIMULATOR_EVENTS_PER_SEC: int = int(
        os.getenv("SIMULATOR_EVENTS_PER_SEC", "10000")
    )

    # Base prices for simulated symbols (Geometric Brownian Motion starting points)
    SIMULATOR_BASE_PRICES: dict = json.loads(os.getenv(
        "SIMULATOR_BASE_PRICES",
        '{"AAPL":175.0,"GOOGL":140.0,"MSFT":380.0,"AMZN":178.0,'
        '"TSLA":245.0,"META":500.0,"NVDA":800.0,"JPM":195.0,"BAC":35.0,"GS":420.0}'
    ))

    # Exchanges for simulated trades
    EXCHANGES: list = ["NYSE", "NASDAQ", "ARCA", "BATS", "IEX"]

    # Alpha Vantage API key (for live mode)
    ALPHA_VANTAGE_API_KEY: str = os.getenv("ALPHA_VANTAGE_API_KEY", "")


settings = Settings()

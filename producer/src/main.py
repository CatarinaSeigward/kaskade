"""
Kafka Producer Application Entry Point.

Orchestrates the market data ingestion pipeline:
1. Initializes the appropriate data source (simulator or live API)
2. Creates the Kafka producer with Schema Registry integration
3. Continuously ingests trade events and publishes to the raw-trades topic

Supports two modes:
- simulator: Generates synthetic market data using GBM + Jump Diffusion
- live: Fetches real market data from Alpha Vantage API
"""

import sys
import signal
import time

import structlog

from config.settings import settings
from .kafka_producer import KafkaProducerWrapper
from .market_simulator import MarketSimulator
from .market_data_fetcher import MarketDataFetcher

# Configure structured logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.dev.ConsoleRenderer(),
    ],
)

logger = structlog.get_logger(__name__)

# Global flag for graceful shutdown
_running = True


def _signal_handler(signum, frame):
    """Handle SIGINT/SIGTERM for graceful shutdown."""
    global _running
    logger.info("shutdown_signal_received", signal=signum)
    _running = False


def _wait_for_kafka(bootstrap_servers: str, max_retries: int = 30) -> None:
    """
    Wait for Kafka brokers to become available before starting the producer.
    Retries with exponential backoff up to max_retries attempts.
    """
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": bootstrap_servers})

    for attempt in range(1, max_retries + 1):
        try:
            metadata = admin.list_topics(timeout=5)
            broker_count = len(metadata.brokers)
            logger.info(
                "kafka_connection_established",
                brokers=broker_count,
                attempt=attempt,
            )
            return
        except Exception as e:
            backoff = min(2 ** attempt, 30)
            logger.warning(
                "kafka_not_ready",
                attempt=attempt,
                max_retries=max_retries,
                backoff=backoff,
                error=str(e),
            )
            time.sleep(backoff)

    logger.error("kafka_connection_failed", max_retries=max_retries)
    sys.exit(1)


def _create_topics_if_needed(bootstrap_servers: str) -> None:
    """
    Create required Kafka topics if they do not already exist.
    Uses AdminClient to create topics with the specified partition and
    replication configuration from the project plan.
    """
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap_servers})

    # Check existing topics
    existing = set(admin.list_topics(timeout=10).topics.keys())

    topics_to_create = []

    # raw-trades: 6 partitions, replication factor 2, 24h retention
    if "raw-trades" not in existing:
        topics_to_create.append(NewTopic(
            "raw-trades",
            num_partitions=6,
            replication_factor=2,
            config={"retention.ms": "86400000"},
        ))

    # enriched-events: 3 partitions, replication factor 2, 7d retention
    if "enriched-events" not in existing:
        topics_to_create.append(NewTopic(
            "enriched-events",
            num_partitions=3,
            replication_factor=2,
            config={"retention.ms": "604800000"},
        ))

    # alerts: 1 partition, replication factor 2, 30d retention
    if "alerts" not in existing:
        topics_to_create.append(NewTopic(
            "alerts",
            num_partitions=1,
            replication_factor=2,
            config={"retention.ms": "2592000000"},
        ))

    if topics_to_create:
        futures = admin.create_topics(topics_to_create)
        for topic_name, future in futures.items():
            try:
                future.result()
                logger.info("topic_created", topic=topic_name)
            except Exception as e:
                logger.warning("topic_creation_skipped", topic=topic_name, reason=str(e))
    else:
        logger.info("all_topics_exist")


def main():
    """Main entry point for the Kafka producer application."""
    global _running

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    logger.info(
        "producer_starting",
        mode=settings.PRODUCER_MODE,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        target_throughput=settings.SIMULATOR_EVENTS_PER_SEC,
    )

    # Wait for Kafka to be ready
    _wait_for_kafka(settings.KAFKA_BOOTSTRAP_SERVERS)

    # Create topics if they don't exist
    _create_topics_if_needed(settings.KAFKA_BOOTSTRAP_SERVERS)

    # Initialize Kafka producer
    producer = KafkaProducerWrapper(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        schema_registry_url=settings.SCHEMA_REGISTRY_URL,
        topic=settings.RAW_TRADES_TOPIC,
    )

    # Initialize data source based on mode
    if settings.PRODUCER_MODE == "live":
        if not settings.ALPHA_VANTAGE_API_KEY:
            logger.error("missing_api_key", mode="live")
            sys.exit(1)

        data_source = MarketDataFetcher(
            api_key=settings.ALPHA_VANTAGE_API_KEY,
            symbols=settings.SIMULATOR_SYMBOLS,
        ).fetch_trades()
        logger.info("live_mode_enabled", api="alpha_vantage")
    else:
        data_source = MarketSimulator(
            symbols=settings.SIMULATOR_SYMBOLS,
            base_prices=settings.SIMULATOR_BASE_PRICES,
            exchanges=settings.EXCHANGES,
            events_per_sec=settings.SIMULATOR_EVENTS_PER_SEC,
        ).generate_trades()
        logger.info("simulator_mode_enabled", symbols=settings.SIMULATOR_SYMBOLS)

    # Main ingestion loop
    event_count = 0
    start_time = time.monotonic()

    try:
        for trade in data_source:
            if not _running:
                break

            producer.produce(trade)
            event_count += 1

            # Periodic flush to ensure timely delivery
            if event_count % 10000 == 0:
                producer.producer.poll(0)

    except KeyboardInterrupt:
        logger.info("keyboard_interrupt_received")
    finally:
        elapsed = time.monotonic() - start_time
        logger.info(
            "producer_shutting_down",
            total_events=event_count,
            elapsed_seconds=round(elapsed, 2),
            avg_throughput=round(event_count / max(elapsed, 0.001), 1),
        )
        producer.close()


if __name__ == "__main__":
    main()

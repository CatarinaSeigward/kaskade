"""
Alert Consumer Application Entry Point.

Initializes the Kafka alert consumer with Redis deduplication and PostgreSQL
persistence. Waits for all dependent services (Kafka, Redis, PostgreSQL) to
become available before starting the consumer loop.
"""

import os
import sys
import time

import structlog

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.dev.ConsoleRenderer(),
    ],
)

logger = structlog.get_logger(__name__)


def _wait_for_services(redis_host: str, redis_port: int, postgres_dsn: str) -> None:
    """Wait for Redis and PostgreSQL to become available."""
    import redis as redis_lib
    import psycopg2

    # Wait for Redis
    for attempt in range(30):
        try:
            r = redis_lib.Redis(host=redis_host, port=redis_port)
            r.ping()
            logger.info("redis_ready", attempt=attempt + 1)
            r.close()
            break
        except Exception:
            time.sleep(2)
    else:
        logger.error("redis_not_available")
        sys.exit(1)

    # Wait for PostgreSQL
    for attempt in range(30):
        try:
            conn = psycopg2.connect(postgres_dsn)
            conn.close()
            logger.info("postgres_ready", attempt=attempt + 1)
            break
        except Exception:
            time.sleep(2)
    else:
        logger.error("postgres_not_available")
        sys.exit(1)


def main():
    """Main entry point for the alert consumer application."""
    kafka_bootstrap = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"
    )
    redis_host = os.getenv("REDIS_HOST", "redis")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    postgres_host = os.getenv("POSTGRES_HOST", "postgres")
    postgres_port = os.getenv("POSTGRES_PORT", "5432")
    postgres_db = os.getenv("POSTGRES_DB", "financial_analytics")
    postgres_user = os.getenv("POSTGRES_USER", "analytics")
    postgres_password = os.getenv("POSTGRES_PASSWORD", "analytics_secure_password")
    grafana_url = os.getenv("GRAFANA_URL", "http://grafana:3000")

    postgres_dsn = (
        f"host={postgres_host} port={postgres_port} "
        f"dbname={postgres_db} user={postgres_user} "
        f"password={postgres_password}"
    )

    logger.info("alert_consumer_starting", kafka=kafka_bootstrap)

    # Wait for dependent services
    _wait_for_services(redis_host, redis_port, postgres_dsn)

    # Import here to avoid circular imports during service wait
    from .consumer import AlertConsumer

    consumer = AlertConsumer(
        kafka_bootstrap=kafka_bootstrap,
        redis_host=redis_host,
        redis_port=redis_port,
        postgres_dsn=postgres_dsn,
        grafana_url=grafana_url,
    )

    consumer.run()


if __name__ == "__main__":
    main()

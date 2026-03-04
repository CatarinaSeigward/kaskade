"""
Alert Consumer - Reads anomaly alerts from Kafka and processes them.

Consumes messages from the 'alerts' Kafka topic, performs deduplication
via Redis (SET NX with TTL), persists to PostgreSQL audit log, and
triggers Grafana webhook annotations for dashboard overlay.

The consumer runs in an infinite loop with graceful shutdown support.
"""

import json
import hashlib
import time
import signal
import sys
from typing import Optional

import redis
import psycopg2
import psycopg2.extras
import requests
from confluent_kafka import Consumer, KafkaError
import structlog

logger = structlog.get_logger(__name__)


class AlertConsumer:
    """
    Kafka consumer for the alerts topic with Redis-based deduplication
    and PostgreSQL persistence.

    Parameters:
        kafka_bootstrap: Kafka broker addresses
        redis_host: Redis server hostname
        redis_port: Redis server port
        postgres_dsn: PostgreSQL connection string
        grafana_url: Grafana base URL for webhook annotations
        group_id: Consumer group ID
        dedup_ttl: Deduplication window in seconds (default: 300 = 5 min)
    """

    def __init__(
        self,
        kafka_bootstrap: str,
        redis_host: str,
        redis_port: int,
        postgres_dsn: str,
        grafana_url: str = "http://grafana:3000",
        group_id: str = "alert-consumer-group",
        dedup_ttl: int = 300,
    ):
        self.dedup_ttl = dedup_ttl
        self.grafana_url = grafana_url
        self._running = True

        # Initialize Kafka consumer
        self.consumer = Consumer({
            "bootstrap.servers": kafka_bootstrap,
            "group.id": group_id,
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 5000,
        })
        self.consumer.subscribe(["alerts"])

        # Initialize Redis for deduplication
        self.redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            decode_responses=True,
        )

        # Initialize PostgreSQL connection
        self.pg_conn = psycopg2.connect(postgres_dsn)
        self.pg_conn.autocommit = True

        # Register signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        logger.info(
            "alert_consumer_initialized",
            kafka=kafka_bootstrap,
            redis=f"{redis_host}:{redis_port}",
        )

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        logger.info("shutdown_signal_received", signal=signum)
        self._running = False

    def _compute_dedup_key(self, alert: dict) -> str:
        """
        Compute a deduplication hash for an alert.
        Uses symbol + alert_type + 5-minute time bucket to deduplicate
        similar alerts within the same time window.
        """
        # Round timestamp to 5-minute bucket
        ts = alert.get("window_start", 0)
        bucket = ts // (self.dedup_ttl * 1000)

        raw = f"{alert['symbol']}:{alert['alert_type']}:{bucket}"
        return f"alert:dedup:{hashlib.md5(raw.encode()).hexdigest()}"

    def _is_duplicate(self, dedup_key: str) -> bool:
        """
        Check if this alert is a duplicate using Redis SET NX with TTL.
        Returns True if the alert was already seen (duplicate).
        """
        # SET NX returns True if the key was set (new alert), False if it existed
        is_new = self.redis_client.set(
            dedup_key, "1", nx=True, ex=self.dedup_ttl
        )
        return not is_new

    def _persist_alert(self, alert: dict) -> None:
        """Write the alert to the PostgreSQL alerts table for audit logging."""
        try:
            with self.pg_conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO alerts (
                        symbol, alert_type, severity, z_score, threshold,
                        current_value, baseline_value, message,
                        window_start, window_end, detected_at
                    ) VALUES (
                        %(symbol)s, %(alert_type)s, %(severity)s,
                        %(z_score)s, %(threshold)s,
                        %(current_value)s, %(baseline_value)s,
                        %(message)s,
                        to_timestamp(%(window_start)s / 1000.0),
                        to_timestamp(%(window_end)s / 1000.0),
                        to_timestamp(%(detected_at)s / 1000.0)
                    )
                    """,
                    alert,
                )
        except Exception as e:
            logger.error("postgres_write_failed", error=str(e))

    def _send_grafana_annotation(self, alert: dict) -> None:
        """
        Send a webhook annotation to Grafana for dashboard overlay.
        Annotations appear as markers on time-series panels.
        """
        try:
            annotation = {
                "dashboardUID": "",  # Empty = global annotation
                "time": alert.get("detected_at", int(time.time() * 1000)),
                "tags": [
                    alert["alert_type"],
                    alert["severity"],
                    alert["symbol"],
                ],
                "text": alert.get("message", "Anomaly detected"),
            }

            response = requests.post(
                f"{self.grafana_url}/api/annotations",
                json=annotation,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": "Bearer admin:admin",  # Basic auth for dev
                },
                timeout=5,
            )

            if response.status_code in (200, 201):
                logger.debug("grafana_annotation_sent", symbol=alert["symbol"])
            else:
                logger.warning(
                    "grafana_annotation_failed",
                    status=response.status_code,
                )

        except requests.exceptions.RequestException as e:
            logger.warning("grafana_webhook_error", error=str(e))

    def process_alert(self, alert: dict) -> bool:
        """
        Process a single alert event:
          1. Check deduplication via Redis
          2. Persist to PostgreSQL if not a duplicate
          3. Send Grafana annotation

        Returns:
            True if the alert was processed (not a duplicate), False otherwise
        """
        dedup_key = self._compute_dedup_key(alert)

        if self._is_duplicate(dedup_key):
            logger.debug(
                "alert_deduplicated",
                symbol=alert["symbol"],
                type=alert["alert_type"],
            )
            return False

        # New alert - persist and annotate
        self._persist_alert(alert)
        self._send_grafana_annotation(alert)

        logger.info(
            "alert_processed",
            symbol=alert["symbol"],
            type=alert["alert_type"],
            severity=alert["severity"],
            z_score=alert.get("z_score"),
        )
        return True

    def run(self) -> None:
        """
        Main consumer loop. Polls Kafka for alert messages and processes them.
        Runs until a shutdown signal is received.
        """
        logger.info("alert_consumer_started")
        processed = 0
        deduplicated = 0

        try:
            while self._running:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("kafka_consumer_error", error=msg.error())
                    continue

                try:
                    # Parse alert JSON from Kafka message value
                    alert = json.loads(msg.value().decode("utf-8"))

                    if self.process_alert(alert):
                        processed += 1
                    else:
                        deduplicated += 1

                except json.JSONDecodeError as e:
                    logger.error("invalid_alert_json", error=str(e))
                except Exception as e:
                    logger.error("alert_processing_error", error=str(e))

        finally:
            logger.info(
                "alert_consumer_shutting_down",
                processed=processed,
                deduplicated=deduplicated,
            )
            self.consumer.close()
            self.pg_conn.close()
            self.redis_client.close()

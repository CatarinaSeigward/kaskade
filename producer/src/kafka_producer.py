"""
Kafka Producer Wrapper with partitioning, batching, and delivery reporting.

Wraps the confluent_kafka Producer with:
- Symbol-based partitioning for ordered per-symbol processing
- Configurable batching (linger.ms, batch.size) for throughput optimization
- Snappy compression for reduced network bandwidth
- Idempotent writes (enable.idempotence=true) for exactly-once semantics
- Delivery callback reporting for monitoring produce success/failure rates
"""

import time
from typing import Optional

from confluent_kafka import Producer, KafkaError
import structlog

from .avro_serializer import AvroTradeSerializer
from config.settings import settings

logger = structlog.get_logger(__name__)


class KafkaProducerWrapper:
    """
    High-throughput Kafka producer for market trade events.

    Handles Avro serialization via Schema Registry, symbol-based partitioning,
    and provides delivery callbacks for monitoring. Configures the producer
    for exactly-once semantics with idempotent writes.

    Parameters:
        bootstrap_servers: Comma-separated Kafka broker addresses
        schema_registry_url: URL of the Confluent Schema Registry
        topic: Target Kafka topic (default: raw-trades)
    """

    def __init__(
        self,
        bootstrap_servers: str,
        schema_registry_url: str,
        topic: str = "raw-trades",
    ):
        self.topic = topic
        self._delivery_count = 0
        self._error_count = 0
        self._last_report_time = time.monotonic()

        # Initialize Avro serializer with Schema Registry
        self.serializer = AvroTradeSerializer(schema_registry_url, topic)

        # Configure Kafka producer with exactly-once semantics
        producer_config = {
            "bootstrap.servers": bootstrap_servers,
            # Exactly-once semantics at producer level
            "acks": settings.PRODUCER_ACKS,
            "enable.idempotence": settings.PRODUCER_ENABLE_IDEMPOTENCE,
            "max.in.flight.requests.per.connection": settings.PRODUCER_MAX_IN_FLIGHT,
            # Batching configuration for throughput
            "linger.ms": settings.PRODUCER_LINGER_MS,
            "batch.size": settings.PRODUCER_BATCH_SIZE,
            # Compression for reduced network bandwidth
            "compression.type": settings.PRODUCER_COMPRESSION_TYPE,
            # Buffer memory
            "buffer.memory": settings.PRODUCER_BUFFER_MEMORY,
            # Client identification
            "client.id": "kaskade-producer",
        }

        self.producer = Producer(producer_config)

        logger.info(
            "kafka_producer_initialized",
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            acks=settings.PRODUCER_ACKS,
            idempotent=settings.PRODUCER_ENABLE_IDEMPOTENCE,
            compression=settings.PRODUCER_COMPRESSION_TYPE,
        )

    def _delivery_callback(self, err: Optional[KafkaError], msg) -> None:
        """
        Callback invoked on successful or failed message delivery.
        Tracks delivery success/failure counts for monitoring.
        """
        if err is not None:
            self._error_count += 1
            logger.error(
                "message_delivery_failed",
                error=str(err),
                topic=msg.topic(),
                partition=msg.partition(),
            )
        else:
            self._delivery_count += 1

        # Log delivery stats every 30 seconds
        now = time.monotonic()
        if now - self._last_report_time >= 30.0:
            logger.info(
                "delivery_stats",
                delivered=self._delivery_count,
                errors=self._error_count,
                error_rate=(
                    f"{self._error_count / max(1, self._delivery_count + self._error_count) * 100:.2f}%"
                ),
            )
            self._last_report_time = now

    def produce(self, trade) -> None:
        """
        Produce a single trade event to Kafka.

        The trade is serialized to Avro format, partitioned by symbol hash,
        and sent asynchronously with a delivery callback.

        Parameters:
            trade: SimulatedTrade or compatible object with symbol, price, etc.
        """
        # Serialize outside try/except so BufferError handler always has 'value'
        try:
            value = self.serializer.serialize(trade)
        except Exception as e:
            logger.error("serialization_error", error=str(e), symbol=trade.symbol)
            raise

        key = trade.symbol.encode("utf-8")

        try:
            # Produce with symbol as key for consistent partition assignment
            self.producer.produce(
                topic=self.topic,
                key=key,
                value=value,
                callback=self._delivery_callback,
            )

            # Trigger delivery callbacks without blocking
            self.producer.poll(0)

        except BufferError:
            # Local producer queue is full; flush and retry
            logger.warning("producer_buffer_full", action="flushing")
            self.producer.flush(timeout=5)
            self.producer.produce(
                topic=self.topic,
                key=key,
                value=value,
                callback=self._delivery_callback,
            )

        except Exception as e:
            logger.error("produce_error", error=str(e), symbol=trade.symbol)
            raise

    def flush(self, timeout: float = 30.0) -> int:
        """
        Flush all outstanding messages and wait for delivery confirmation.

        Returns:
            Number of messages still in the queue (0 = all delivered)
        """
        remaining = self.producer.flush(timeout=timeout)
        if remaining > 0:
            logger.warning("flush_incomplete", remaining_messages=remaining)
        return remaining

    def close(self) -> None:
        """Flush remaining messages and close the producer."""
        logger.info("producer_closing", pending_messages=len(self.producer))
        self.flush()
        logger.info(
            "producer_closed",
            total_delivered=self._delivery_count,
            total_errors=self._error_count,
        )

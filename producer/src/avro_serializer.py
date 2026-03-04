"""
Avro Serializer with Confluent Schema Registry integration.

Handles serialization of trade events into Avro binary format using the
Confluent Schema Registry for centralized schema management. Supports
automatic schema resolution and backward compatibility enforcement.
"""

import json
import os

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer as ConfluentAvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

import structlog

logger = structlog.get_logger(__name__)

# Path to Avro schema files.
# In Docker: /app/schemas/ (WORKDIR is /app)
# Locally: ../../schemas/ relative to this file
_WORKDIR_SCHEMA = os.path.join(os.getcwd(), "schemas")
_RELATIVE_SCHEMA = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "schemas",
)
SCHEMA_DIR = _WORKDIR_SCHEMA if os.path.isdir(_WORKDIR_SCHEMA) else _RELATIVE_SCHEMA


def _load_schema(schema_file: str) -> str:
    """Load an Avro schema from the schemas directory."""
    schema_path = os.path.join(SCHEMA_DIR, schema_file)
    with open(schema_path, "r") as f:
        return f.read()


def _trade_to_dict(trade, ctx) -> dict:
    """
    Convert a SimulatedTrade object to a dictionary matching the RawTrade Avro schema.
    This function is passed to the Confluent AvroSerializer as the to_dict callback.
    """
    return {
        "symbol": trade.symbol,
        "price": trade.price,
        "volume": trade.volume,
        "timestamp": trade.timestamp,
        "exchange": trade.exchange,
        "trade_id": trade.trade_id,
    }


class AvroTradeSerializer:
    """
    Serializes trade events into Avro format using Confluent Schema Registry.

    The serializer registers the RawTrade schema on first use and caches the
    schema ID for subsequent serializations. Schema compatibility mode is set
    to BACKWARD at the registry level.

    Parameters:
        schema_registry_url: URL of the Confluent Schema Registry
        topic: Kafka topic name (used for subject naming: {topic}-value)
    """

    def __init__(self, schema_registry_url: str, topic: str = "raw-trades"):
        self.topic = topic

        # Initialize Schema Registry client
        self.registry_client = SchemaRegistryClient({
            "url": schema_registry_url,
        })

        # Load and register the RawTrade Avro schema
        raw_trade_schema_str = _load_schema("raw_trade.avsc")

        self.serializer = ConfluentAvroSerializer(
            schema_registry_client=self.registry_client,
            schema_str=raw_trade_schema_str,
            to_dict=_trade_to_dict,
            conf={"auto.register.schemas": True},
        )

        logger.info(
            "avro_serializer_initialized",
            schema_registry_url=schema_registry_url,
            topic=topic,
        )

    def serialize(self, trade) -> bytes:
        """
        Serialize a trade event to Avro binary format.

        Returns:
            Avro-encoded bytes with Schema Registry wire format
            (magic byte + 4-byte schema ID + Avro payload)
        """
        ctx = SerializationContext(self.topic, MessageField.VALUE)
        return self.serializer(trade, ctx)

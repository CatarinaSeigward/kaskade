#!/bin/bash
# ============================================================
# Kafka Topic Creation Script
# Creates the three core topics with appropriate partitioning
# and retention policies for the analytics platform.
# ============================================================

set -e

KAFKA_BOOTSTRAP="kafka-1:9092"

echo "Waiting for Kafka cluster to be ready..."
sleep 10

# Create raw-trades topic
# - 6 partitions for parallel processing (partitioned by symbol hash)
# - Replication factor 2 for fault tolerance
# - 24-hour retention for raw tick data
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP \
  --create --if-not-exists \
  --topic raw-trades \
  --partitions 6 \
  --replication-factor 2 \
  --config retention.ms=86400000 \
  --config cleanup.policy=delete \
  --config max.message.bytes=1048576

echo "Created topic: raw-trades (6 partitions, 24h retention)"

# Create enriched-events topic
# - 3 partitions (lower cardinality after window aggregation)
# - 7-day retention for enriched analytics data
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP \
  --create --if-not-exists \
  --topic enriched-events \
  --partitions 3 \
  --replication-factor 2 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete

echo "Created topic: enriched-events (3 partitions, 7d retention)"

# Create alerts topic
# - 1 partition (low volume, ordering matters)
# - 30-day retention for alert audit trail
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP \
  --create --if-not-exists \
  --topic alerts \
  --partitions 1 \
  --replication-factor 2 \
  --config retention.ms=2592000000 \
  --config cleanup.policy=delete

echo "Created topic: alerts (1 partition, 30d retention)"

# Verify topic creation
echo ""
echo "=== Topic List ==="
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP --list

echo ""
echo "=== Topic Details ==="
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP --describe --topic raw-trades
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP --describe --topic enriched-events
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP --describe --topic alerts

echo ""
echo "All topics created successfully."

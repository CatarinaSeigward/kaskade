#!/bin/bash
# ============================================================
# Register Avro Schemas with Confluent Schema Registry
# ============================================================

set -e

SCHEMA_REGISTRY_URL=${SCHEMA_REGISTRY_URL:-http://localhost:8081}

echo "Registering Avro schemas with Schema Registry at $SCHEMA_REGISTRY_URL"
echo ""

# Register raw-trades value schema
echo "Registering raw-trades-value schema..."
curl -s -X POST \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "{\"schema\": $(cat schemas/raw_trade.avsc | python3 -c 'import sys,json; print(json.dumps(sys.stdin.read()))')}" \
  "$SCHEMA_REGISTRY_URL/subjects/raw-trades-value/versions" | python3 -m json.tool

echo ""

# Register enriched-events value schema
echo "Registering enriched-events-value schema..."
curl -s -X POST \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "{\"schema\": $(cat schemas/enriched_event.avsc | python3 -c 'import sys,json; print(json.dumps(sys.stdin.read()))')}" \
  "$SCHEMA_REGISTRY_URL/subjects/enriched-events-value/versions" | python3 -m json.tool

echo ""

# Register alerts value schema
echo "Registering alerts-value schema..."
curl -s -X POST \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "{\"schema\": $(cat schemas/alert.avsc | python3 -c 'import sys,json; print(json.dumps(sys.stdin.read()))')}" \
  "$SCHEMA_REGISTRY_URL/subjects/alerts-value/versions" | python3 -m json.tool

echo ""

# List registered subjects
echo "Registered subjects:"
curl -s "$SCHEMA_REGISTRY_URL/subjects" | python3 -m json.tool

echo ""
echo "Schema registration complete."

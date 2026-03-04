#!/bin/bash
# ============================================================
# Quick Start Script
# Starts the full Kaskade platform
# ============================================================

set -e

echo "============================================"
echo "  Kaskade - Real-Time Market Analytics"
echo "============================================"
echo ""

# Copy .env.example if .env doesn't exist
if [ ! -f .env ]; then
    echo "Creating .env from .env.example..."
    cp .env.example .env
fi

# Start infrastructure services first
echo "[1/4] Starting infrastructure (Kafka, Redis, PostgreSQL, Schema Registry)..."
docker compose up -d kafka-1 kafka-2 kafka-3 schema-registry redis postgres

echo "[2/4] Waiting for services to become healthy..."
sleep 20

# Check Kafka health
echo "  Checking Kafka..."
for i in $(seq 1 30); do
    if docker exec kafka-1 kafka-broker-api-versions.sh --bootstrap-server localhost:9092 > /dev/null 2>&1; then
        echo "  Kafka is ready."
        break
    fi
    sleep 2
done

# Check PostgreSQL health
echo "  Checking PostgreSQL..."
for i in $(seq 1 15); do
    if docker exec postgres pg_isready -U analytics > /dev/null 2>&1; then
        echo "  PostgreSQL is ready."
        break
    fi
    sleep 2
done

# Check Redis health
echo "  Checking Redis..."
for i in $(seq 1 10); do
    if docker exec redis redis-cli ping > /dev/null 2>&1; then
        echo "  Redis is ready."
        break
    fi
    sleep 1
done

# Start Grafana and Kafka UI
echo "[3/4] Starting Grafana and Kafka UI..."
docker compose up -d grafana kafka-ui

# Start application services
echo "[4/4] Starting application services (Producer, API, Alert Consumer)..."
docker compose up -d producer api alert-consumer

echo ""
echo "============================================"
echo "Kaskade is starting up!"
echo "============================================"
echo ""
echo "Service URLs:"
echo "  Kafka UI:     http://localhost:8080"
echo "  Grafana:      http://localhost:3000  (admin/admin)"
echo "  REST API:     http://localhost:8000"
echo "  API Docs:     http://localhost:8000/docs"
echo "  Spark Master: http://localhost:8082"
echo ""
echo "Useful commands:"
echo "  docker compose logs -f producer    # Watch producer output"
echo "  docker compose logs -f api         # Watch API logs"
echo "  docker compose ps                  # Check service status"
echo "  docker compose down                # Stop all services"
echo ""

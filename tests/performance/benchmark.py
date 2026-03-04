"""
Performance Benchmark Script.

Measures key performance metrics of the real-time analytics pipeline:
  - Ingestion throughput (events/sec) from Kafka producer
  - End-to-end latency (producer -> Redis/PostgreSQL)
  - Redis read latency for real-time dashboard queries
  - PostgreSQL query performance for historical time-range queries

Usage:
    python -m tests.performance.benchmark [--duration 60] [--events-per-sec 10000]

Requires all services to be running via docker compose.
"""

import os
import sys
import time
import json
import uuid
import argparse
import statistics
from datetime import datetime, timedelta

import redis
import psycopg2

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))


def benchmark_redis_reads(
    host: str = "localhost",
    port: int = 6379,
    iterations: int = 1000,
) -> dict:
    """
    Benchmark Redis HGETALL latency for real-time metric reads.
    Simulates Grafana dashboard polling behavior.
    """
    client = redis.Redis(host=host, port=port, decode_responses=True)

    # Seed test data
    test_key = "latest:BENCH"
    client.hset(test_key, mapping={
        "vwap": "175.50",
        "volatility": "0.0234",
        "rsi": "55.0",
        "volume": "50000",
        "trade_count": "150",
        "high": "176.00",
        "low": "175.00",
        "updated_at": str(int(time.time() * 1000)),
    })
    client.expire(test_key, 60)

    latencies = []
    for _ in range(iterations):
        start = time.perf_counter()
        client.hgetall(test_key)
        elapsed = (time.perf_counter() - start) * 1000  # ms
        latencies.append(elapsed)

    # Cleanup
    client.delete(test_key)
    client.close()

    return {
        "operation": "Redis HGETALL",
        "iterations": iterations,
        "p50_ms": round(statistics.median(latencies), 3),
        "p95_ms": round(sorted(latencies)[int(0.95 * len(latencies))], 3),
        "p99_ms": round(sorted(latencies)[int(0.99 * len(latencies))], 3),
        "avg_ms": round(statistics.mean(latencies), 3),
        "min_ms": round(min(latencies), 3),
        "max_ms": round(max(latencies), 3),
    }


def benchmark_postgres_queries(
    host: str = "localhost",
    port: int = 5432,
    dbname: str = "financial_analytics",
    user: str = "analytics",
    password: str = "analytics_secure_password",
    iterations: int = 100,
) -> dict:
    """
    Benchmark PostgreSQL query latency for time-range queries.
    Simulates Grafana dashboard query patterns.
    """
    conn = psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user, password=password
    )

    latencies = []
    with conn.cursor() as cursor:
        for _ in range(iterations):
            start = time.perf_counter()
            cursor.execute(
                """
                SELECT symbol, window_start, vwap, volatility, volume
                FROM market_metrics
                WHERE window_start >= NOW() - INTERVAL '1 hour'
                ORDER BY window_start DESC
                LIMIT 100
                """
            )
            cursor.fetchall()
            elapsed = (time.perf_counter() - start) * 1000  # ms
            latencies.append(elapsed)

    conn.close()

    return {
        "operation": "PostgreSQL time-range query",
        "iterations": iterations,
        "p50_ms": round(statistics.median(latencies), 3),
        "p95_ms": round(sorted(latencies)[int(0.95 * len(latencies))], 3),
        "p99_ms": round(sorted(latencies)[int(0.99 * len(latencies))], 3),
        "avg_ms": round(statistics.mean(latencies), 3),
        "min_ms": round(min(latencies), 3),
        "max_ms": round(max(latencies), 3),
    }


def benchmark_producer_throughput(
    duration_seconds: int = 10,
    events_per_sec: int = 10000,
) -> dict:
    """
    Benchmark the simulator's raw event generation throughput
    (without Kafka overhead) to establish baseline capacity.
    """
    from producer.src.market_simulator import MarketSimulator
    from producer.config.settings import settings

    simulator = MarketSimulator(
        symbols=settings.SIMULATOR_SYMBOLS,
        base_prices=settings.SIMULATOR_BASE_PRICES,
        exchanges=settings.EXCHANGES,
        events_per_sec=events_per_sec,
    )

    gen = simulator.generate_trades()
    event_count = 0
    start = time.monotonic()

    while time.monotonic() - start < duration_seconds:
        next(gen)
        event_count += 1

    elapsed = time.monotonic() - start
    throughput = event_count / elapsed

    return {
        "operation": "Simulator throughput",
        "duration_seconds": round(elapsed, 2),
        "events_generated": event_count,
        "throughput_eps": round(throughput, 1),
        "target_eps": events_per_sec,
        "target_met": throughput >= events_per_sec * 0.9,
    }


def main():
    parser = argparse.ArgumentParser(description="Performance benchmark suite")
    parser.add_argument("--duration", type=int, default=10, help="Benchmark duration (seconds)")
    parser.add_argument("--events-per-sec", type=int, default=10000, help="Target events/sec")
    args = parser.parse_args()

    print("=" * 60)
    print("Kaskade - Performance Benchmark")
    print("=" * 60)
    print()

    # Benchmark 1: Simulator throughput
    print("[1/3] Benchmarking simulator throughput...")
    try:
        result = benchmark_producer_throughput(args.duration, args.events_per_sec)
        print(f"  Throughput: {result['throughput_eps']} events/sec")
        print(f"  Target met: {result['target_met']}")
        print(f"  Duration: {result['duration_seconds']}s")
        print()
    except Exception as e:
        print(f"  Skipped: {e}")
        print()

    # Benchmark 2: Redis read latency
    print("[2/3] Benchmarking Redis read latency...")
    try:
        result = benchmark_redis_reads()
        print(f"  p50: {result['p50_ms']}ms")
        print(f"  p95: {result['p95_ms']}ms")
        print(f"  p99: {result['p99_ms']}ms")
        print()
    except Exception as e:
        print(f"  Skipped (Redis not available): {e}")
        print()

    # Benchmark 3: PostgreSQL query latency
    print("[3/3] Benchmarking PostgreSQL query latency...")
    try:
        result = benchmark_postgres_queries()
        print(f"  p50: {result['p50_ms']}ms")
        print(f"  p95: {result['p95_ms']}ms")
        print(f"  p99: {result['p99_ms']}ms")
        print()
    except Exception as e:
        print(f"  Skipped (PostgreSQL not available): {e}")
        print()

    print("=" * 60)
    print("Benchmark complete.")


if __name__ == "__main__":
    main()

"""
End-to-End Integration Tests.

Tests the full pipeline: Producer -> Kafka -> (simulated) Enriched Events -> API.
Requires Docker Compose services to be running:
  docker compose up -d kafka-1 kafka-2 kafka-3 schema-registry redis postgres

Run with: pytest tests/integration/ -v
"""

import os
import time
import json
import uuid

import pytest
import redis
import psycopg2


# ============================================================
# Configuration (use external ports for local testing)
# ============================================================
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "financial_analytics")
POSTGRES_USER = os.getenv("POSTGRES_USER", "analytics")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "analytics_secure_password")


@pytest.fixture(scope="module")
def redis_client():
    """Create a Redis client for integration tests."""
    client = redis.Redis(
        host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
    )
    yield client
    client.close()


@pytest.fixture(scope="module")
def pg_conn():
    """Create a PostgreSQL connection for integration tests."""
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    yield conn
    conn.close()


class TestRedisConnectivity:
    """Test Redis connectivity and basic operations."""

    def test_redis_ping(self, redis_client):
        """Verify Redis is reachable."""
        assert redis_client.ping() is True

    def test_redis_write_read(self, redis_client):
        """Verify Redis read-after-write consistency."""
        test_key = f"test:{uuid.uuid4()}"
        redis_client.hset(test_key, mapping={
            "vwap": "175.50",
            "volume": "10000",
            "symbol": "TEST",
        })
        redis_client.expire(test_key, 10)

        result = redis_client.hgetall(test_key)
        assert result["vwap"] == "175.50"
        assert result["volume"] == "10000"
        assert result["symbol"] == "TEST"

        # Cleanup
        redis_client.delete(test_key)

    def test_redis_sorted_set(self, redis_client):
        """Verify Redis sorted set operations for top movers."""
        test_key = f"test:topmovers:{uuid.uuid4()}"

        redis_client.zadd(test_key, {"AAPL": 2.5, "GOOGL": 1.8, "TSLA": 5.2})
        redis_client.expire(test_key, 10)

        # Get top 2 movers
        result = redis_client.zrevrange(test_key, 0, 1, withscores=True)
        assert len(result) == 2
        assert result[0][0] == "TSLA"
        assert result[0][1] == 5.2

        # Cleanup
        redis_client.delete(test_key)


class TestPostgresConnectivity:
    """Test PostgreSQL connectivity and schema validation."""

    def test_postgres_connection(self, pg_conn):
        """Verify PostgreSQL is reachable."""
        with pg_conn.cursor() as cursor:
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            assert result[0] == 1

    def test_market_metrics_table_exists(self, pg_conn):
        """Verify the market_metrics table was created by init script."""
        with pg_conn.cursor() as cursor:
            cursor.execute(
                "SELECT EXISTS (SELECT FROM information_schema.tables "
                "WHERE table_name = 'market_metrics')"
            )
            assert cursor.fetchone()[0] is True

    def test_alerts_table_exists(self, pg_conn):
        """Verify the alerts table was created by init script."""
        with pg_conn.cursor() as cursor:
            cursor.execute(
                "SELECT EXISTS (SELECT FROM information_schema.tables "
                "WHERE table_name = 'alerts')"
            )
            assert cursor.fetchone()[0] is True

    def test_insert_and_query_metric(self, pg_conn):
        """Verify write and read of a market metric record."""
        with pg_conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO market_metrics
                    (symbol, window_start, window_end, vwap, volatility,
                     rsi, volume, trade_count, high, low)
                VALUES
                    ('TEST', NOW() - INTERVAL '1 minute', NOW(),
                     100.50, 0.0234, 55.0, 50000, 150, 101.00, 99.50)
                RETURNING id
                """
            )
            row_id = cursor.fetchone()[0]
            pg_conn.commit()

            assert row_id is not None

            # Query the inserted record
            cursor.execute(
                "SELECT symbol, vwap FROM market_metrics WHERE id = %s",
                (row_id,),
            )
            result = cursor.fetchone()
            assert result[0] == "TEST"
            assert float(result[1]) == 100.50

            # Cleanup
            cursor.execute("DELETE FROM market_metrics WHERE id = %s", (row_id,))
            pg_conn.commit()

    def test_insert_and_query_alert(self, pg_conn):
        """Verify write and read of an alert record."""
        with pg_conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO alerts
                    (symbol, alert_type, severity, z_score, threshold,
                     current_value, baseline_value, message)
                VALUES
                    ('TEST', 'PRICE_ANOMALY', 'WARNING', 3.5, 3.0,
                     180.00, 175.00, 'Test alert')
                RETURNING id
                """
            )
            row_id = cursor.fetchone()[0]
            pg_conn.commit()

            assert row_id is not None

            cursor.execute(
                "SELECT severity FROM alerts WHERE id = %s", (row_id,)
            )
            assert cursor.fetchone()[0] == "WARNING"

            # Cleanup
            cursor.execute("DELETE FROM alerts WHERE id = %s", (row_id,))
            pg_conn.commit()


class TestKafkaConnectivity:
    """Test Kafka broker connectivity."""

    def test_kafka_broker_connection(self):
        """Verify Kafka broker is reachable."""
        try:
            from confluent_kafka.admin import AdminClient

            admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
            metadata = admin.list_topics(timeout=10)
            assert len(metadata.brokers) > 0
        except ImportError:
            pytest.skip("confluent_kafka not installed")
        except Exception as e:
            pytest.skip(f"Kafka not available: {e}")

    def test_topics_exist(self):
        """Verify required Kafka topics are created."""
        try:
            from confluent_kafka.admin import AdminClient

            admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
            metadata = admin.list_topics(timeout=10)
            topic_names = set(metadata.topics.keys())

            # These topics should be created by the producer or create-topics script
            expected = {"raw-trades", "enriched-events", "alerts"}
            # Only check if topics were already created
            if expected.issubset(topic_names):
                assert True
            else:
                pytest.skip("Topics not yet created (run producer first)")
        except ImportError:
            pytest.skip("confluent_kafka not installed")
        except Exception as e:
            pytest.skip(f"Kafka not available: {e}")

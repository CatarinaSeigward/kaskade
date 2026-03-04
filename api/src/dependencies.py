"""
Shared dependencies for FastAPI application.

Provides connection pools and clients for Redis and PostgreSQL that are
shared across API route handlers via FastAPI's dependency injection system.
"""

import os
from contextlib import contextmanager
from typing import Generator

import redis
import psycopg2
import psycopg2.extras
import psycopg2.pool
import structlog

logger = structlog.get_logger(__name__)

# Redis connection pool (singleton)
_redis_pool = None

# PostgreSQL connection pool (singleton)
_pg_pool = None


def get_redis() -> redis.Redis:
    """
    Get a Redis client from the connection pool.
    Creates the pool on first call (lazy initialization).
    """
    global _redis_pool
    if _redis_pool is None:
        _redis_pool = redis.ConnectionPool(
            host=os.getenv("REDIS_HOST", "redis"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            db=0,
            decode_responses=True,
            max_connections=10,
        )
    return redis.Redis(connection_pool=_redis_pool)


def get_pg_connection():
    """
    Get a PostgreSQL connection from the connection pool.
    Creates the pool on first call (lazy initialization).
    """
    global _pg_pool
    if _pg_pool is None:
        _pg_pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=10,
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname=os.getenv("POSTGRES_DB", "financial_analytics"),
            user=os.getenv("POSTGRES_USER", "analytics"),
            password=os.getenv("POSTGRES_PASSWORD", "analytics_secure_password"),
        )
    return _pg_pool.getconn()


def release_pg_connection(conn) -> None:
    """Return a PostgreSQL connection to the pool."""
    global _pg_pool
    if _pg_pool is not None:
        _pg_pool.putconn(conn)


@contextmanager
def pg_cursor() -> Generator:
    """
    Context manager for PostgreSQL cursor with automatic connection management.
    Ensures the connection is returned to the pool after use.
    """
    conn = get_pg_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            yield cursor
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        release_pg_connection(conn)


def check_kafka_health(bootstrap_servers: str) -> bool:
    """Check Kafka broker connectivity."""
    try:
        from confluent_kafka.admin import AdminClient
        admin = AdminClient({"bootstrap.servers": bootstrap_servers})
        metadata = admin.list_topics(timeout=5)
        return len(metadata.brokers) > 0
    except Exception:
        return False


def check_redis_health() -> bool:
    """Check Redis connectivity."""
    try:
        r = get_redis()
        return r.ping()
    except Exception:
        return False


def check_postgres_health() -> bool:
    """Check PostgreSQL connectivity."""
    try:
        conn = get_pg_connection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
        release_pg_connection(conn)
        return True
    except Exception:
        return False

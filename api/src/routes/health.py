"""
Health Check API Route.

Provides a comprehensive health endpoint that checks connectivity
to all dependent services: Kafka, Redis, and PostgreSQL.
Used by Docker health checks and operational monitoring.
"""

import os
from datetime import datetime

from fastapi import APIRouter

from ..models import HealthResponse
from ..dependencies import check_redis_health, check_postgres_health, check_kafka_health

router = APIRouter(tags=["health"])


@router.get(
    "/api/v1/health",
    response_model=HealthResponse,
    summary="Service health check",
    description="Returns connectivity status for all dependent services. "
                "Used by Docker health checks and Grafana operational monitoring.",
)
def health_check():
    """
    Check connectivity to Kafka, Redis, and PostgreSQL.
    Returns overall status: 'healthy' if all services are reachable,
    'degraded' if any service is unavailable.
    """
    kafka_bootstrap = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092"
    )

    kafka_ok = check_kafka_health(kafka_bootstrap)
    redis_ok = check_redis_health()
    postgres_ok = check_postgres_health()

    all_healthy = kafka_ok and redis_ok and postgres_ok

    return HealthResponse(
        status="healthy" if all_healthy else "degraded",
        kafka="connected" if kafka_ok else "disconnected",
        redis="connected" if redis_ok else "disconnected",
        postgres="connected" if postgres_ok else "disconnected",
        timestamp=datetime.utcnow(),
    )

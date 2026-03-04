"""
FastAPI Application Entry Point.

Exposes REST API endpoints for programmatic access to real-time market
metrics, historical data, alerts, and service health status.

API documentation is auto-generated at /docs (Swagger UI) and /redoc.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import structlog

from .routes import metrics, alerts, health

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.dev.ConsoleRenderer(),
    ],
)

# Initialize FastAPI application
app = FastAPI(
    title="Kaskade API",
    description=(
        "REST API for real-time market metrics, historical analytics, "
        "and anomaly detection alerts. Serves data from Redis (real-time) "
        "and PostgreSQL (historical) backends."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS middleware for Grafana and frontend access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register route modules
app.include_router(metrics.router)
app.include_router(alerts.router)
app.include_router(health.router)


@app.get("/", tags=["root"])
def root():
    """API root endpoint with navigation links."""
    return {
        "service": "Kaskade API",
        "version": "1.0.0",
        "endpoints": {
            "docs": "/docs",
            "health": "/api/v1/health",
            "latest_metrics": "/api/v1/metrics/{symbol}/latest",
            "historical_metrics": "/api/v1/metrics/{symbol}/history",
            "alerts": "/api/v1/alerts",
            "top_movers": "/api/v1/metrics/topmovers",
        },
    }

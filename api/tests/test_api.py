"""
Unit tests for FastAPI REST API endpoints.

Tests the API response models and basic endpoint behavior
using FastAPI's TestClient (no external services required for
model validation tests).
"""

import pytest
from datetime import datetime

from api.src.models import (
    MetricsResponse,
    HistoricalMetricsResponse,
    AlertResponse,
    TopMoverResponse,
    HealthResponse,
)


class TestModels:
    """Test Pydantic response model validation."""

    def test_metrics_response_valid(self):
        """Verify MetricsResponse accepts valid data."""
        response = MetricsResponse(
            symbol="AAPL",
            vwap=175.50,
            volatility=0.0234,
            rsi=55.0,
            volume=50000,
            trade_count=150,
            high=176.00,
            low=175.00,
        )
        assert response.symbol == "AAPL"
        assert response.vwap == 175.50
        assert response.rsi == 55.0

    def test_historical_metrics_response(self):
        """Verify HistoricalMetricsResponse accepts valid data."""
        response = HistoricalMetricsResponse(
            symbol="GOOGL",
            window_start=datetime(2026, 3, 1, 10, 0, 0),
            window_end=datetime(2026, 3, 1, 10, 1, 0),
            vwap=140.25,
            volatility=0.0156,
            rsi=62.5,
            volume=30000,
            trade_count=80,
            high=141.00,
            low=139.50,
        )
        assert response.symbol == "GOOGL"
        assert response.window_start.year == 2026

    def test_alert_response(self):
        """Verify AlertResponse accepts valid data."""
        response = AlertResponse(
            id=1,
            symbol="TSLA",
            alert_type="PRICE_ANOMALY",
            severity="WARNING",
            z_score=3.5,
            threshold=3.0,
            current_value=250.00,
            baseline_value=245.00,
            message="TSLA VWAP anomaly detected",
            detected_at=datetime(2026, 3, 1, 10, 5, 0),
            acknowledged=False,
        )
        assert response.severity == "WARNING"
        assert response.z_score == 3.5
        assert response.acknowledged is False

    def test_top_mover_response(self):
        """Verify TopMoverResponse accepts valid data."""
        response = TopMoverResponse(
            symbol="NVDA",
            price_change_pct=5.23,
        )
        assert response.symbol == "NVDA"
        assert response.price_change_pct == 5.23

    def test_health_response(self):
        """Verify HealthResponse accepts valid data."""
        response = HealthResponse(
            status="healthy",
            kafka="connected",
            redis="connected",
            postgres="connected",
            timestamp=datetime.utcnow(),
        )
        assert response.status == "healthy"

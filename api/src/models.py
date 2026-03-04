"""
Pydantic response models for the FastAPI REST API.

Defines strongly-typed response schemas for all API endpoints. These models
are used for response serialization, validation, and auto-generated OpenAPI
documentation.
"""

from datetime import datetime
from typing import Optional, List

from pydantic import BaseModel, Field


class MetricsResponse(BaseModel):
    """Response model for real-time metrics from Redis."""

    symbol: str = Field(description="Ticker symbol")
    vwap: float = Field(description="Volume-Weighted Average Price")
    volatility: float = Field(description="Price volatility (std dev)")
    rsi: float = Field(description="Relative Strength Index (0-100)")
    volume: int = Field(description="Total trade volume in window")
    trade_count: int = Field(description="Number of trades in window")
    high: float = Field(description="Highest price in window")
    low: float = Field(description="Lowest price in window")
    window_start: Optional[int] = Field(default=None, description="Window start epoch ms")
    window_end: Optional[int] = Field(default=None, description="Window end epoch ms")
    updated_at: Optional[int] = Field(default=None, description="Last update epoch ms")


class HistoricalMetricsResponse(BaseModel):
    """Response model for historical metrics from PostgreSQL."""

    symbol: str
    window_start: datetime
    window_end: datetime
    vwap: float
    volatility: float
    rsi: float
    volume: int
    trade_count: int
    high: float
    low: float


class AlertResponse(BaseModel):
    """Response model for anomaly alerts."""

    id: int
    symbol: str
    alert_type: str
    severity: str
    z_score: float
    threshold: float
    current_value: Optional[float] = None
    baseline_value: Optional[float] = None
    message: str
    window_start: Optional[datetime] = None
    window_end: Optional[datetime] = None
    detected_at: datetime
    acknowledged: bool


class TopMoverResponse(BaseModel):
    """Response model for top market movers."""

    symbol: str
    price_change_pct: float = Field(description="Price change percentage")


class HealthResponse(BaseModel):
    """Response model for service health check."""

    status: str = Field(description="Overall service status")
    kafka: str = Field(description="Kafka connectivity status")
    redis: str = Field(description="Redis connectivity status")
    postgres: str = Field(description="PostgreSQL connectivity status")
    timestamp: datetime = Field(description="Health check timestamp")


class PaginatedResponse(BaseModel):
    """Generic paginated response wrapper."""

    data: List[dict]
    total: int
    page: int
    page_size: int

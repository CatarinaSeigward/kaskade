"""
Metrics API Routes.

Provides endpoints for querying real-time and historical market metrics.
Real-time data is served from Redis for sub-millisecond latency.
Historical data is queried from PostgreSQL with time-range filtering.
"""

from datetime import datetime, timedelta
from typing import List, Optional

from fastapi import APIRouter, Query, HTTPException

from ..models import MetricsResponse, HistoricalMetricsResponse, TopMoverResponse
from ..dependencies import get_redis, pg_cursor

router = APIRouter(prefix="/api/v1/metrics", tags=["metrics"])


# IMPORTANT: Static routes (/topmovers) must be defined BEFORE parameterized
# routes (/{symbol}/...) to prevent FastAPI from capturing "topmovers" as a
# symbol path parameter.

@router.get(
    "/topmovers",
    response_model=List[TopMoverResponse],
    summary="Get top market movers",
    description="Returns the top N symbols ranked by price change percentage "
                "from the Redis sorted set.",
)
def get_top_movers(
    n: int = Query(default=10, ge=1, le=50, description="Number of top movers"),
):
    """Retrieve top movers ranked by price change from Redis."""
    r = get_redis()

    # Get top N from sorted set (highest score = biggest price change)
    movers = r.zrevrange("topmovers", 0, n - 1, withscores=True)

    if not movers:
        return []

    return [
        TopMoverResponse(
            symbol=symbol,
            price_change_pct=round(score, 4),
        )
        for symbol, score in movers
    ]


@router.get(
    "/{symbol}/latest",
    response_model=MetricsResponse,
    summary="Get latest real-time metrics for a symbol",
    description="Returns the most recent windowed metrics from Redis cache. "
                "Data is updated every 10 seconds by the Spark streaming pipeline.",
)
def get_latest_metrics(symbol: str):
    """Retrieve the latest real-time metrics for a symbol from Redis."""
    r = get_redis()
    key = f"latest:{symbol.upper()}"
    data = r.hgetall(key)

    if not data:
        raise HTTPException(
            status_code=404,
            detail=f"No metrics found for symbol: {symbol.upper()}",
        )

    return MetricsResponse(
        symbol=symbol.upper(),
        vwap=float(data.get("vwap", 0)),
        volatility=float(data.get("volatility", 0)),
        rsi=float(data.get("rsi", 50)),
        volume=int(float(data.get("volume", 0))),
        trade_count=int(float(data.get("trade_count", 0))),
        high=float(data.get("high", 0)),
        low=float(data.get("low", 0)),
        window_start=int(float(data.get("window_start", 0))) if data.get("window_start") else None,
        window_end=int(float(data.get("window_end", 0))) if data.get("window_end") else None,
        updated_at=int(float(data.get("updated_at", 0))) if data.get("updated_at") else None,
    )


@router.get(
    "/{symbol}/history",
    response_model=List[HistoricalMetricsResponse],
    summary="Get historical metrics for a symbol",
    description="Returns historical windowed metrics from PostgreSQL "
                "with configurable time range filtering.",
)
def get_historical_metrics(
    symbol: str,
    start: Optional[datetime] = Query(
        default=None,
        description="Start time (ISO 8601). Defaults to 1 hour ago.",
    ),
    end: Optional[datetime] = Query(
        default=None,
        description="End time (ISO 8601). Defaults to now.",
    ),
    limit: int = Query(
        default=100,
        ge=1,
        le=1000,
        description="Maximum number of records to return",
    ),
):
    """Retrieve historical metrics for a symbol from PostgreSQL."""
    if start is None:
        start = datetime.utcnow() - timedelta(hours=1)
    if end is None:
        end = datetime.utcnow()

    with pg_cursor() as cursor:
        cursor.execute(
            """
            SELECT symbol, window_start, window_end, vwap, volatility,
                   rsi, volume, trade_count, high, low
            FROM market_metrics
            WHERE symbol = %s
              AND window_start >= %s
              AND window_start <= %s
            ORDER BY window_start DESC
            LIMIT %s
            """,
            (symbol.upper(), start, end, limit),
        )
        rows = cursor.fetchall()

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No historical metrics found for symbol: {symbol.upper()}",
        )

    return [
        HistoricalMetricsResponse(
            symbol=row["symbol"],
            window_start=row["window_start"],
            window_end=row["window_end"],
            vwap=float(row["vwap"]),
            volatility=float(row["volatility"]),
            rsi=float(row["rsi"]),
            volume=int(row["volume"]),
            trade_count=int(row["trade_count"]),
            high=float(row["high"]),
            low=float(row["low"]),
        )
        for row in rows
    ]

"""
Alerts API Routes.

Provides endpoints for querying anomaly detection alerts with filtering
by severity, symbol, and time range. Data is served from PostgreSQL.
"""

from datetime import datetime, timedelta
from typing import List, Optional

from fastapi import APIRouter, Query, HTTPException

from ..models import AlertResponse
from ..dependencies import pg_cursor

router = APIRouter(prefix="/api/v1/alerts", tags=["alerts"])


@router.get(
    "",
    response_model=List[AlertResponse],
    summary="Get recent alerts",
    description="Returns recent anomaly detection alerts with optional filtering "
                "by severity, symbol, and alert type.",
)
def get_alerts(
    symbol: Optional[str] = Query(default=None, description="Filter by ticker symbol"),
    severity: Optional[str] = Query(
        default=None,
        description="Filter by severity: INFO, WARNING, CRITICAL",
    ),
    alert_type: Optional[str] = Query(
        default=None,
        description="Filter by type: PRICE_ANOMALY, VOLUME_SPIKE, VOLATILITY_SPIKE",
    ),
    hours: int = Query(
        default=24,
        ge=1,
        le=168,
        description="Look back window in hours (max 7 days)",
    ),
    limit: int = Query(
        default=50,
        ge=1,
        le=500,
        description="Maximum number of alerts to return",
    ),
):
    """Retrieve recent alerts with optional filtering."""
    # Build dynamic query with parameterized filters
    conditions = ["detected_at >= %s"]
    params = [datetime.utcnow() - timedelta(hours=hours)]

    if symbol:
        conditions.append("symbol = %s")
        params.append(symbol.upper())

    if severity:
        if severity.upper() not in ("INFO", "WARNING", "CRITICAL"):
            raise HTTPException(
                status_code=400,
                detail="severity must be one of: INFO, WARNING, CRITICAL",
            )
        conditions.append("severity = %s")
        params.append(severity.upper())

    if alert_type:
        valid_types = ("PRICE_ANOMALY", "VOLUME_SPIKE", "VOLATILITY_SPIKE")
        if alert_type.upper() not in valid_types:
            raise HTTPException(
                status_code=400,
                detail=f"alert_type must be one of: {', '.join(valid_types)}",
            )
        conditions.append("alert_type = %s")
        params.append(alert_type.upper())

    where_clause = " AND ".join(conditions)
    params.append(limit)

    with pg_cursor() as cursor:
        cursor.execute(
            f"""
            SELECT id, symbol, alert_type, severity, z_score, threshold,
                   current_value, baseline_value, message,
                   window_start, window_end, detected_at, acknowledged
            FROM alerts
            WHERE {where_clause}
            ORDER BY detected_at DESC
            LIMIT %s
            """,
            params,
        )
        rows = cursor.fetchall()

    return [
        AlertResponse(
            id=row["id"],
            symbol=row["symbol"],
            alert_type=row["alert_type"],
            severity=row["severity"],
            z_score=float(row["z_score"]) if row["z_score"] else 0.0,
            threshold=float(row["threshold"]) if row["threshold"] else 0.0,
            current_value=float(row["current_value"]) if row["current_value"] else None,
            baseline_value=float(row["baseline_value"]) if row["baseline_value"] else None,
            message=row["message"],
            window_start=row["window_start"],
            window_end=row["window_end"],
            detected_at=row["detected_at"],
            acknowledged=row["acknowledged"],
        )
        for row in rows
    ]


@router.get(
    "/summary",
    summary="Get alert summary statistics",
    description="Returns aggregated alert counts grouped by symbol and severity "
                "for the overview dashboard.",
)
def get_alert_summary(
    hours: int = Query(default=24, ge=1, le=168),
):
    """Get alert counts grouped by symbol and severity."""
    with pg_cursor() as cursor:
        cursor.execute(
            """
            SELECT symbol, severity, alert_type,
                   COUNT(*) as alert_count,
                   MAX(detected_at) as last_detected
            FROM alerts
            WHERE detected_at >= %s
            GROUP BY symbol, severity, alert_type
            ORDER BY alert_count DESC
            """,
            (datetime.utcnow() - timedelta(hours=hours),),
        )
        rows = cursor.fetchall()

    return [dict(row) for row in rows]

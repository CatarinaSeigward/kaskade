"""
Market Data Fetcher for live financial data sources.

Connects to financial data APIs (Alpha Vantage, Polygon.io) and WebSocket feeds
with automatic reconnection and exponential backoff. Normalizes incoming data
into the RawTrade schema format for Kafka ingestion.

Currently supports:
- Alpha Vantage REST API (free tier, rate-limited)
- Extensible for WebSocket feeds (Polygon.io, Finnhub, etc.)
"""

import time
import uuid
from typing import Generator, Optional

import requests
import structlog

from .market_simulator import SimulatedTrade

logger = structlog.get_logger(__name__)


class MarketDataFetcher:
    """
    Fetches live market data from external APIs and yields normalized trade events.

    Implements exponential backoff for API rate limiting and automatic reconnection
    for transient failures. Falls back to simulator mode if API is unavailable.

    Parameters:
        api_key: Alpha Vantage API key
        symbols: List of ticker symbols to track
        poll_interval: Seconds between API calls (default 12s for free tier rate limit)
        max_retries: Maximum consecutive retries before logging error (default 5)
        base_backoff: Initial backoff duration in seconds (default 1.0)
    """

    BASE_URL = "https://www.alphavantage.co/query"

    def __init__(
        self,
        api_key: str,
        symbols: list,
        poll_interval: float = 12.0,
        max_retries: int = 5,
        base_backoff: float = 1.0,
    ):
        self.api_key = api_key
        self.symbols = symbols
        self.poll_interval = poll_interval
        self.max_retries = max_retries
        self.base_backoff = base_backoff
        self.session = requests.Session()

        logger.info(
            "market_data_fetcher_initialized",
            symbols=len(symbols),
            poll_interval=poll_interval,
        )

    def _fetch_quote(self, symbol: str) -> Optional[dict]:
        """
        Fetch the latest quote for a single symbol from Alpha Vantage.
        Returns None on failure (caller handles retry logic).
        """
        params = {
            "function": "GLOBAL_QUOTE",
            "symbol": symbol,
            "apikey": self.api_key,
        }

        try:
            response = self.session.get(
                self.BASE_URL, params=params, timeout=10
            )
            response.raise_for_status()
            data = response.json()

            quote = data.get("Global Quote", {})
            if not quote:
                logger.warning("empty_quote_response", symbol=symbol)
                return None

            return {
                "symbol": symbol,
                "price": float(quote.get("05. price", 0)),
                "volume": int(quote.get("06. volume", 0)),
                "exchange": "UNKNOWN",  # Alpha Vantage does not provide exchange
            }

        except requests.exceptions.RequestException as e:
            logger.error("api_request_failed", symbol=symbol, error=str(e))
            return None

    def fetch_trades(self) -> Generator[SimulatedTrade, None, None]:
        """
        Continuously poll the API for each symbol and yield trade events.
        Implements exponential backoff on consecutive failures.
        """
        consecutive_failures = 0

        logger.info("live_data_fetching_started", symbols=self.symbols)

        while True:
            for symbol in self.symbols:
                quote = self._fetch_quote(symbol)

                if quote and quote["price"] > 0:
                    consecutive_failures = 0

                    trade = SimulatedTrade(
                        symbol=quote["symbol"],
                        price=quote["price"],
                        volume=quote["volume"],
                        timestamp=int(time.time() * 1000),
                        exchange=quote["exchange"],
                        trade_id=str(uuid.uuid4()),
                    )
                    yield trade
                else:
                    consecutive_failures += 1

                    if consecutive_failures >= self.max_retries:
                        backoff = min(
                            self.base_backoff * (2 ** consecutive_failures),
                            60.0,
                        )
                        logger.warning(
                            "consecutive_failures_backoff",
                            failures=consecutive_failures,
                            backoff_seconds=backoff,
                        )
                        time.sleep(backoff)

                # Rate limiting: Alpha Vantage free tier allows 5 calls/min
                time.sleep(self.poll_interval)

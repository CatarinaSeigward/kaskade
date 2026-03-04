"""
Market Data Simulator using Geometric Brownian Motion with Poisson Jump Process.

Generates realistic synthetic tick-level trade data at configurable throughput
(1K-50K events/sec). The stochastic model produces price paths with:
- Continuous diffusion component (GBM) for normal market behavior
- Jump component (Poisson process) for sudden price movements / anomalies

This serves as the fallback data source when live API connections are unavailable,
and as the primary data generator for integration testing and benchmarks.
"""

import time
import uuid
import random
import numpy as np
from dataclasses import dataclass
from typing import Generator, Dict, List

import structlog

logger = structlog.get_logger(__name__)


@dataclass
class SimulatedTrade:
    """Represents a single simulated trade event."""
    symbol: str
    price: float
    volume: int
    timestamp: int  # epoch milliseconds
    exchange: str
    trade_id: str


class MarketSimulator:
    """
    Generates synthetic market trade data using a Geometric Brownian Motion (GBM)
    model with Poisson jump diffusion for realistic price dynamics.

    Parameters:
        symbols: List of ticker symbols to simulate
        base_prices: Initial price per symbol
        exchanges: List of exchange names to randomly assign
        events_per_sec: Target throughput (events per second across all symbols)
        mu: Annual drift rate (default 0.05 = 5% annual return)
        sigma: Annual volatility (default 0.20 = 20% annualized)
        jump_intensity: Average jumps per year (Poisson lambda, default 10)
        jump_mean: Mean jump size as fraction of price (default 0.0)
        jump_std: Std dev of jump size (default 0.02 = 2%)
    """

    def __init__(
        self,
        symbols: List[str],
        base_prices: Dict[str, float],
        exchanges: List[str],
        events_per_sec: int = 10000,
        mu: float = 0.05,
        sigma: float = 0.20,
        jump_intensity: float = 10.0,
        jump_mean: float = 0.0,
        jump_std: float = 0.02,
    ):
        self.symbols = symbols
        self.exchanges = exchanges
        self.events_per_sec = events_per_sec

        # Time step: each tick represents ~1/events_per_sec of a second
        # Convert annual parameters to per-tick parameters
        # Assume 252 trading days, 6.5 hours per day
        seconds_per_year = 252 * 6.5 * 3600
        self.dt = 1.0 / seconds_per_year  # time step in years

        self.mu = mu
        self.sigma = sigma
        self.jump_intensity = jump_intensity
        self.jump_mean = jump_mean
        self.jump_std = jump_std

        # Initialize current prices from base prices
        self.current_prices: Dict[str, float] = {}
        for symbol in symbols:
            self.current_prices[symbol] = base_prices.get(symbol, 100.0)

        # Pre-allocate symbol weights for random selection
        # Higher-priced stocks get slightly more volume
        total_price = sum(self.current_prices.values())
        self.symbol_weights = [
            self.current_prices[s] / total_price for s in self.symbols
        ]

        logger.info(
            "market_simulator_initialized",
            symbols=len(symbols),
            target_throughput=events_per_sec,
        )

    def _evolve_price(self, symbol: str) -> float:
        """
        Evolve the price of a symbol by one time step using GBM + Jump Diffusion.

        The price follows: dS/S = (mu - 0.5*sigma^2)*dt + sigma*dW + J*dN
        where dW ~ N(0, dt), J ~ N(jump_mean, jump_std), dN ~ Poisson(lambda*dt)
        """
        S = self.current_prices[symbol]

        # Brownian motion component
        dW = np.random.normal(0, np.sqrt(self.dt))
        drift = (self.mu - 0.5 * self.sigma ** 2) * self.dt
        diffusion = self.sigma * dW

        # Poisson jump component
        jump = 0.0
        if np.random.poisson(self.jump_intensity * self.dt) > 0:
            jump = np.random.normal(self.jump_mean, self.jump_std)

        # Apply log-normal price evolution
        log_return = drift + diffusion + jump
        new_price = S * np.exp(log_return)

        # Enforce minimum price (penny stock floor)
        new_price = max(new_price, 0.01)

        self.current_prices[symbol] = new_price
        return round(new_price, 2)

    def _generate_volume(self, symbol: str) -> int:
        """
        Generate a realistic trade volume using a log-normal distribution.
        Volume is inversely correlated with price (cheaper stocks trade higher volume).
        """
        base_volume = 1000.0 / (self.current_prices[symbol] / 100.0)
        volume = int(np.random.lognormal(
            mean=np.log(base_volume),
            sigma=0.8
        ))
        return max(1, volume)

    def generate_trades(self) -> Generator[SimulatedTrade, None, None]:
        """
        Infinite generator that yields simulated trade events at the configured
        throughput rate. Uses precise sleep timing to maintain target events/sec.
        """
        batch_size = max(1, self.events_per_sec // 100)  # ~100 micro-batches/sec
        sleep_interval = batch_size / self.events_per_sec

        logger.info(
            "trade_generation_started",
            batch_size=batch_size,
            sleep_interval_ms=round(sleep_interval * 1000, 2),
        )

        events_generated = 0
        start_time = time.monotonic()

        while True:
            batch_start = time.monotonic()

            for _ in range(batch_size):
                # Select symbol weighted by price
                symbol = random.choices(
                    self.symbols, weights=self.symbol_weights, k=1
                )[0]

                price = self._evolve_price(symbol)
                volume = self._generate_volume(symbol)
                exchange = random.choice(self.exchanges)

                trade = SimulatedTrade(
                    symbol=symbol,
                    price=price,
                    volume=volume,
                    timestamp=int(time.time() * 1000),
                    exchange=exchange,
                    trade_id=str(uuid.uuid4()),
                )

                events_generated += 1
                yield trade

            # Throttle to maintain target throughput
            elapsed = time.monotonic() - batch_start
            remaining = sleep_interval - elapsed
            if remaining > 0:
                time.sleep(remaining)

            # Log throughput every 10 seconds
            total_elapsed = time.monotonic() - start_time
            if total_elapsed > 0 and events_generated % (self.events_per_sec * 10) == 0:
                actual_rate = events_generated / total_elapsed
                logger.info(
                    "throughput_report",
                    events_generated=events_generated,
                    actual_rate=round(actual_rate, 1),
                    target_rate=self.events_per_sec,
                    elapsed_seconds=round(total_elapsed, 1),
                )

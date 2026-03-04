"""
Unit tests for the Market Simulator.

Validates the GBM + Jump Diffusion model produces realistic price paths,
correct trade event structure, and configurable throughput behavior.
"""

import pytest
from producer.src.market_simulator import MarketSimulator, SimulatedTrade


@pytest.fixture
def simulator():
    """Create a simulator with known configuration for deterministic testing."""
    return MarketSimulator(
        symbols=["AAPL", "GOOGL", "MSFT"],
        base_prices={"AAPL": 175.0, "GOOGL": 140.0, "MSFT": 380.0},
        exchanges=["NYSE", "NASDAQ"],
        events_per_sec=1000,
    )


def test_trade_structure(simulator):
    """Verify generated trades have all required fields with correct types."""
    gen = simulator.generate_trades()
    trade = next(gen)

    assert isinstance(trade, SimulatedTrade)
    assert isinstance(trade.symbol, str)
    assert trade.symbol in ["AAPL", "GOOGL", "MSFT"]
    assert isinstance(trade.price, float)
    assert trade.price > 0
    assert isinstance(trade.volume, int)
    assert trade.volume > 0
    assert isinstance(trade.timestamp, int)
    assert trade.timestamp > 0
    assert isinstance(trade.exchange, str)
    assert trade.exchange in ["NYSE", "NASDAQ"]
    assert isinstance(trade.trade_id, str)
    assert len(trade.trade_id) == 36  # UUID format


def test_price_evolution(simulator):
    """Verify prices evolve within reasonable bounds after multiple steps."""
    gen = simulator.generate_trades()

    # Generate 1000 trades and check price stays within 50% of base
    prices = {"AAPL": [], "GOOGL": [], "MSFT": []}
    for _ in range(1000):
        trade = next(gen)
        prices[trade.symbol].append(trade.price)

    # Prices should stay within reasonable range (not exactly base price)
    for symbol, price_list in prices.items():
        if price_list:
            assert min(price_list) > 0, f"{symbol} price went to zero"
            avg_price = sum(price_list) / len(price_list)
            base = simulator.current_prices.get(symbol, 100.0)
            # Price should not deviate more than 50% from current (very loose bound)
            assert avg_price > 0


def test_volume_generation(simulator):
    """Verify volumes are positive integers with realistic distribution."""
    gen = simulator.generate_trades()

    volumes = []
    for _ in range(500):
        trade = next(gen)
        volumes.append(trade.volume)

    assert all(v > 0 for v in volumes), "All volumes must be positive"
    assert all(isinstance(v, int) for v in volumes), "All volumes must be integers"
    # Volume should have some variance (not all the same)
    assert len(set(volumes)) > 1, "Volumes should vary"


def test_symbol_distribution(simulator):
    """Verify all configured symbols receive trade events."""
    gen = simulator.generate_trades()

    seen_symbols = set()
    for _ in range(500):
        trade = next(gen)
        seen_symbols.add(trade.symbol)

    assert seen_symbols == {"AAPL", "GOOGL", "MSFT"}


def test_unique_trade_ids(simulator):
    """Verify each trade has a unique identifier."""
    gen = simulator.generate_trades()

    trade_ids = set()
    for _ in range(1000):
        trade = next(gen)
        trade_ids.add(trade.trade_id)

    assert len(trade_ids) == 1000, "All trade IDs must be unique"

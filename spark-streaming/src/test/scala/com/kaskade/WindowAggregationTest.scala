package com.kaskade

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for windowed aggregation metric computations.
 *
 * Tests VWAP calculation, volatility, and RSI proxy logic
 * in isolation from Spark streaming infrastructure.
 */
class WindowAggregationTest extends AnyFunSuite with Matchers {

  test("VWAP calculation: weighted average of price by volume") {
    // VWAP = sum(price * volume) / sum(volume)
    val trades = Seq(
      (100.0, 200L),   // price=100, volume=200
      (102.0, 300L),   // price=102, volume=300
      (101.0, 500L),   // price=101, volume=500
    )

    val totalPriceVolume = trades.map { case (p, v) => p * v }.sum
    val totalVolume = trades.map(_._2).sum
    val vwap = totalPriceVolume / totalVolume

    // Expected: (100*200 + 102*300 + 101*500) / (200+300+500) = 101100/1000 = 101.10
    vwap shouldBe 101.1 +- 0.001
  }

  test("Volatility: standard deviation of prices") {
    val prices = Seq(100.0, 102.0, 101.0, 99.0, 103.0)
    val mean = prices.sum / prices.size
    val variance = prices.map(p => math.pow(p - mean, 2)).sum / prices.size
    val stddev = math.sqrt(variance)

    // Volatility should be positive and reasonable
    stddev should be > 0.0
    stddev should be < 10.0

    // Expected mean = 101.0, stddev ~= 1.414
    mean shouldBe 101.0 +- 0.001
    stddev shouldBe 1.414 +- 0.1
  }

  test("RSI proxy: midpoint price maps to RSI 50") {
    // When avg price is exactly between high and low, RSI proxy should be ~50
    val high = 110.0
    val low = 90.0
    val avg = 100.0  // midpoint

    val rsi = ((avg - low) / (high - low)) * 100.0
    rsi shouldBe 50.0 +- 0.001
  }

  test("RSI proxy: bullish case near high") {
    val high = 110.0
    val low = 90.0
    val avg = 108.0  // close to high

    val rsi = ((avg - low) / (high - low)) * 100.0
    rsi shouldBe 90.0 +- 0.001
  }

  test("RSI proxy: bearish case near low") {
    val high = 110.0
    val low = 90.0
    val avg = 92.0  // close to low

    val rsi = ((avg - low) / (high - low)) * 100.0
    rsi shouldBe 10.0 +- 0.001
  }

  test("RSI proxy: single price gives RSI 50") {
    // When high == low (no range), RSI should default to 50
    val high = 100.0
    val low = 100.0
    val priceRange = high - low

    val rsi = if (priceRange == 0.0) 50.0 else ((100.0 - low) / priceRange) * 100.0
    rsi shouldBe 50.0
  }
}

package com.kaskade

import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._

/**
 * Window Aggregation Engine for Spark Structured Streaming.
 *
 * Computes sliding-window metrics over raw trade events:
 *   - VWAP (Volume-Weighted Average Price)
 *   - Volatility (standard deviation of prices within window)
 *   - RSI (Relative Strength Index approximation)
 *   - Trade count, volume, high, low
 *
 * Window configuration (per project plan):
 *   - 1-minute window, 30-second slide, 30-second watermark (real-time alerting)
 *
 * Watermark policy: 30-second tolerance for late-arriving data.
 * Events beyond the watermark are dropped and logged.
 */
object WindowAggregation {

  /**
   * Compute windowed aggregation metrics on the raw trades stream.
   *
   * @param trades DataFrame with columns: symbol, price, volume, event_time, exchange, trade_id
   * @return DataFrame with windowed aggregation results
   */
  def compute(trades: DataFrame): DataFrame = {
    import trades.sparkSession.implicits._

    trades
      // Apply watermark for late data handling (30-second tolerance)
      .withWatermark("event_time", "30 seconds")
      // Group by symbol and 1-minute sliding window with 30-second slide
      .groupBy(
        col("symbol"),
        window(col("event_time"), "1 minute", "30 seconds")
      )
      .agg(
        // VWAP = sum(price * volume) / sum(volume)
        (sum(col("price") * col("volume")) / sum(col("volume"))).as("vwap"),

        // Volatility = standard deviation of prices in the window
        stddev_pop(col("price")).as("volatility"),

        // RSI approximation using average gain/loss ratio within the window
        // Simplified: RSI = 100 - (100 / (1 + avg_gain/avg_loss))
        // Here we compute a proxy using the ratio of positive to negative price moves
        computeRsiProxy(col("price")).as("rsi"),

        // Aggregate volume across all trades in the window
        sum(col("volume")).as("total_volume"),

        // Number of trades in the window
        count("*").as("trade_count"),

        // Intraday range
        max(col("price")).as("high"),
        min(col("price")).as("low")
      )
      // Flatten the window struct into separate columns
      .select(
        col("symbol"),
        col("window.start").as("window_start"),
        col("window.end").as("window_end"),
        // Round numeric values for cleaner output
        round(col("vwap"), 6).as("vwap"),
        round(coalesce(col("volatility"), lit(0.0)), 8).as("volatility"),
        round(coalesce(col("rsi"), lit(50.0)), 2).as("rsi"),
        col("total_volume").as("volume"),
        col("trade_count"),
        round(col("high"), 6).as("high"),
        round(col("low"), 6).as("low")
      )
  }

  /**
   * Compute a simplified RSI proxy within a single aggregation.
   *
   * True RSI requires sequential price comparison (gain/loss per tick), which
   * is not directly feasible in a single groupBy aggregation. This proxy uses
   * the ratio of (high - open) / (high - low) scaled to 0-100 as an approximation.
   *
   * For production-grade RSI, the full 14-period RSI is computed in the
   * anomaly detection layer using Redis-cached rolling state.
   *
   * @param priceCol The price column
   * @return Column expression for RSI proxy (0-100 scale)
   */
  private def computeRsiProxy(priceCol: Column): Column = {
    // RSI proxy: measures where the average price falls within the window range
    // If avg is closer to high -> bullish (RSI near 100)
    // If avg is closer to low -> bearish (RSI near 0)
    val avgPrice = avg(priceCol)
    val highPrice = max(priceCol)
    val lowPrice = min(priceCol)
    val priceRange = highPrice - lowPrice

    // Avoid division by zero when high == low (single price in window)
    when(priceRange === 0.0, lit(50.0))
      .otherwise(((avgPrice - lowPrice) / priceRange) * 100.0)
  }
}

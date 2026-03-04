package com.kaskade

import org.apache.spark.sql.DataFrame
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig, Pipeline}

/**
 * Redis Sink for real-time state updates from Spark Structured Streaming.
 *
 * Updates the following Redis data structures on each micro-batch:
 *
 *   latest:{symbol}  (Hash)       - Latest VWAP, volume, price, timestamp (TTL: 60s)
 *   history:{symbol} (Sorted Set) - Rolling window metrics scored by timestamp (TTL: 1h)
 *   topmovers        (Sorted Set) - Top symbols by absolute price change (TTL: 60s)
 *
 * Uses Redis pipelining to batch multiple commands per round-trip for throughput.
 */
object RedisSink {

  private val LATEST_TTL = 60          // 60 seconds TTL for latest metrics
  private val HISTORY_TTL = 3600       // 1 hour TTL for rolling history
  private val TOP_MOVERS_TTL = 60      // 60 seconds TTL for top movers

  /**
   * Write a batch of windowed aggregation results to Redis.
   *
   * For each row in the batch:
   *   1. Update the latest:{symbol} hash with current metrics
   *   2. Add to history:{symbol} sorted set for rolling analysis
   *   3. Update the topmovers sorted set with price change magnitude
   *
   * @param batchDF DataFrame with columns: symbol, window_start, window_end,
   *                vwap, volatility, rsi, volume, trade_count, high, low
   * @param redisHost Redis server hostname
   * @param redisPort Redis server port
   */
  def writeBatch(batchDF: DataFrame, redisHost: String, redisPort: Int): Unit = {
    // Collect batch to driver (acceptable for state updates; batch size is bounded)
    val rows = batchDF.collect()
    if (rows.isEmpty) return

    val poolConfig = new JedisPoolConfig()
    poolConfig.setMaxTotal(8)
    poolConfig.setMaxIdle(4)

    val pool = new JedisPool(poolConfig, redisHost, redisPort)

    try {
      val jedis = pool.getResource
      try {
        // Use pipelining for batch efficiency
        val pipe: Pipeline = jedis.pipelined()

        for (row <- rows) {
          val symbol = row.getAs[String]("symbol")
          val vwap = row.getAs[Double]("vwap")
          val volatility = row.getAs[Double]("volatility")
          val rsi = row.getAs[Double]("rsi")
          val volume = row.getAs[Long]("volume")
          val tradeCount = row.getAs[Long]("trade_count")
          val high = row.getAs[Double]("high")
          val low = row.getAs[Double]("low")
          val windowStart = row.getAs[java.sql.Timestamp]("window_start")
          val windowEnd = row.getAs[java.sql.Timestamp]("window_end")

          val latestKey = s"latest:$symbol"
          val historyKey = s"history:$symbol"

          // Update latest:{symbol} hash with current window metrics
          val fields = new java.util.HashMap[String, String]()
          fields.put("vwap", vwap.toString)
          fields.put("volatility", volatility.toString)
          fields.put("rsi", rsi.toString)
          fields.put("volume", volume.toString)
          fields.put("trade_count", tradeCount.toString)
          fields.put("high", high.toString)
          fields.put("low", low.toString)
          fields.put("window_start", windowStart.getTime.toString)
          fields.put("window_end", windowEnd.getTime.toString)
          fields.put("updated_at", System.currentTimeMillis().toString)

          pipe.hset(latestKey, fields)
          pipe.expire(latestKey, LATEST_TTL.toLong)

          // Add to history sorted set (score = window_end timestamp for ordering)
          val historyValue = s"$vwap|$volatility|$rsi|$volume|$tradeCount|$high|$low"
          pipe.zadd(historyKey, windowEnd.getTime.toDouble, historyValue)
          pipe.expire(historyKey, HISTORY_TTL.toLong)

          // Trim history to keep only recent entries (last 120 windows = ~1 hour at 30s slide)
          pipe.zremrangeByRank(historyKey, 0, -121)

          // Update top movers by price range percentage
          val priceRange = if (low > 0) ((high - low) / low) * 100.0 else 0.0
          pipe.zadd("topmovers", priceRange, symbol)
          pipe.expire("topmovers", TOP_MOVERS_TTL.toLong)
        }

        // Execute all pipelined commands
        pipe.sync()

      } finally {
        jedis.close()
      }
    } finally {
      pool.close()
    }
  }
}

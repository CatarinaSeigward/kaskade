package com.kaskade

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}
import scala.collection.mutable.ListBuffer

/**
 * Multi-signal Anomaly Detection Engine.
 *
 * Implements three anomaly detection signals using Z-score and threshold analysis:
 *   1. Price Anomaly: Z-score of current VWAP vs. 20-window rolling mean/std
 *   2. Volume Anomaly: Current volume vs. 5x rolling mean threshold
 *   3. Volatility Spike: Current volatility vs. 3x 1-hour rolling average
 *
 * Rolling statistics are maintained in Redis sorted sets (score = timestamp)
 * to avoid Spark state store overhead and enable cross-query state sharing.
 *
 * Alert severity classification:
 *   INFO:     |Z-score| > 2.0
 *   WARNING:  |Z-score| > 3.0
 *   CRITICAL: |Z-score| > 4.0
 */
object AnomalyDetection {

  private val ROLLING_WINDOW_SIZE = 20    // Number of windows for rolling stats
  private val PRICE_ZSCORE_THRESHOLD = 3.0
  private val VOLUME_MULTIPLIER = 5.0
  private val VOLATILITY_MULTIPLIER = 3.0

  /**
   * Alert data class for structured anomaly event representation.
   */
  case class AlertEvent(
    symbol: String,
    alertType: String,
    severity: String,
    zScore: Double,
    threshold: Double,
    currentValue: Double,
    baselineValue: Double,
    message: String,
    windowStart: java.sql.Timestamp,
    windowEnd: java.sql.Timestamp,
    detectedAt: java.sql.Timestamp
  ) {
    def toRow: Row = Row(
      symbol, alertType, severity, zScore, threshold,
      currentValue, baselineValue, message,
      windowStart, windowEnd, detectedAt
    )
  }

  /** Schema for alert DataFrame creation. */
  val alertSchema: StructType = StructType(Seq(
    StructField("symbol", StringType, nullable = false),
    StructField("alert_type", StringType, nullable = false),
    StructField("severity", StringType, nullable = false),
    StructField("z_score", DoubleType, nullable = false),
    StructField("threshold", DoubleType, nullable = false),
    StructField("current_value", DoubleType, nullable = false),
    StructField("baseline_value", DoubleType, nullable = false),
    StructField("message", StringType, nullable = false),
    StructField("window_start", TimestampType, nullable = false),
    StructField("window_end", TimestampType, nullable = false),
    StructField("detected_at", TimestampType, nullable = false)
  ))

  /**
   * Detect anomalies in a batch of windowed aggregation results.
   *
   * For each row in the batch:
   *   1. Retrieve rolling statistics from Redis
   *   2. Compute Z-scores for price, volume, and volatility
   *   3. Generate alerts for values exceeding thresholds
   *   4. Update Redis with current values for future comparisons
   *
   * @param batchDF DataFrame with windowed aggregation results
   * @param redisHost Redis server hostname
   * @param redisPort Redis server port
   * @return List of AlertEvent objects for detected anomalies
   */
  def detect(batchDF: DataFrame, redisHost: String, redisPort: Int): List[AlertEvent] = {
    val alerts = ListBuffer[AlertEvent]()
    val pool = new JedisPool(new JedisPoolConfig(), redisHost, redisPort)

    try {
      val jedis = pool.getResource
      try {
        val rows = batchDF.collect()

        for (row <- rows) {
          val symbol = row.getAs[String]("symbol")
          val vwap = row.getAs[Double]("vwap")
          val volume = row.getAs[Long]("volume")
          val volatility = row.getAs[Double]("volatility")
          val windowStart = row.getAs[java.sql.Timestamp]("window_start")
          val windowEnd = row.getAs[java.sql.Timestamp]("window_end")
          val now = new java.sql.Timestamp(System.currentTimeMillis())

          // --- Price Anomaly Detection ---
          val priceKey = s"rolling:price:$symbol"
          val priceHistory = getHistory(jedis, priceKey)
          if (priceHistory.nonEmpty) {
            val (mean, std) = computeStats(priceHistory)
            if (std > 0) {
              val zScore = (vwap - mean) / std
              if (math.abs(zScore) > PRICE_ZSCORE_THRESHOLD) {
                val severity = classifySeverity(math.abs(zScore))
                alerts += AlertEvent(
                  symbol = symbol,
                  alertType = "PRICE_ANOMALY",
                  severity = severity,
                  zScore = math.round(zScore * 10000.0) / 10000.0,
                  threshold = PRICE_ZSCORE_THRESHOLD,
                  currentValue = vwap,
                  baselineValue = math.round(mean * 1000000.0) / 1000000.0,
                  message = s"$symbol VWAP anomaly: Z-score=${f"$zScore%.4f"}, " +
                    s"current=$$${"%.2f".format(vwap)}, baseline=$$${"%.2f".format(mean)}",
                  windowStart = windowStart,
                  windowEnd = windowEnd,
                  detectedAt = now
                )
              }
            }
          }
          // Update price rolling history
          updateHistory(jedis, priceKey, vwap, windowEnd.getTime)

          // --- Volume Anomaly Detection ---
          val volumeKey = s"rolling:volume:$symbol"
          val volumeHistory = getHistory(jedis, volumeKey)
          if (volumeHistory.nonEmpty) {
            val (mean, _) = computeStats(volumeHistory)
            if (mean > 0 && volume > mean * VOLUME_MULTIPLIER) {
              val ratio = volume / mean
              val severity = if (ratio > 10) "CRITICAL" else if (ratio > 7) "WARNING" else "INFO"
              alerts += AlertEvent(
                symbol = symbol,
                alertType = "VOLUME_SPIKE",
                severity = severity,
                zScore = math.round(ratio * 10000.0) / 10000.0,
                threshold = VOLUME_MULTIPLIER,
                currentValue = volume.toDouble,
                baselineValue = math.round(mean * 100.0) / 100.0,
                message = s"$symbol volume spike: ${f"$ratio%.1f"}x above average " +
                  s"(current=$volume, avg=${mean.toLong})",
                windowStart = windowStart,
                windowEnd = windowEnd,
                detectedAt = now
              )
            }
          }
          updateHistory(jedis, volumeKey, volume.toDouble, windowEnd.getTime)

          // --- Volatility Spike Detection ---
          val volKey = s"rolling:volatility:$symbol"
          val volHistory = getHistory(jedis, volKey)
          if (volHistory.nonEmpty) {
            val (mean, _) = computeStats(volHistory)
            if (mean > 0 && volatility > mean * VOLATILITY_MULTIPLIER) {
              val ratio = volatility / mean
              val severity = if (ratio > 6) "CRITICAL" else if (ratio > 4) "WARNING" else "INFO"
              alerts += AlertEvent(
                symbol = symbol,
                alertType = "VOLATILITY_SPIKE",
                severity = severity,
                zScore = math.round(ratio * 10000.0) / 10000.0,
                threshold = VOLATILITY_MULTIPLIER,
                currentValue = math.round(volatility * 100000000.0) / 100000000.0,
                baselineValue = math.round(mean * 100000000.0) / 100000000.0,
                message = s"$symbol volatility spike: ${f"$ratio%.1f"}x above rolling average",
                windowStart = windowStart,
                windowEnd = windowEnd,
                detectedAt = now
              )
            }
          }
          updateHistory(jedis, volKey, volatility, windowEnd.getTime)
        }
      } finally {
        jedis.close()
      }
    } finally {
      pool.close()
    }

    alerts.toList
  }

  /**
   * Retrieve rolling history values from a Redis sorted set.
   * Returns the last ROLLING_WINDOW_SIZE values ordered by timestamp (score).
   */
  private def getHistory(jedis: Jedis, key: String): Seq[Double] = {
    val entries = jedis.zrangeWithScores(key, -ROLLING_WINDOW_SIZE, -1)
    val result = new ListBuffer[Double]()
    val iter = entries.iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      result += entry.getElement.toDouble
    }
    result.toSeq
  }

  /**
   * Add a new value to the Redis sorted set history.
   * Trims to keep only the last ROLLING_WINDOW_SIZE entries.
   */
  private def updateHistory(jedis: Jedis, key: String, value: Double, timestamp: Long): Unit = {
    jedis.zadd(key, timestamp.toDouble, value.toString)
    // Trim to keep only the most recent entries
    val size = jedis.zcard(key)
    if (size > ROLLING_WINDOW_SIZE * 2) {
      jedis.zremrangeByRank(key, 0, (size - ROLLING_WINDOW_SIZE - 1).toInt)
    }
    // Set TTL of 2 hours for automatic cleanup
    jedis.expire(key, 7200)
  }

  /**
   * Compute mean and standard deviation of a sequence of values.
   */
  private def computeStats(values: Seq[Double]): (Double, Double) = {
    if (values.isEmpty) return (0.0, 0.0)
    val mean = values.sum / values.size
    val variance = values.map(v => math.pow(v - mean, 2)).sum / values.size
    (mean, math.sqrt(variance))
  }

  /**
   * Classify alert severity based on Z-score magnitude.
   *   INFO:     |Z| > 2.0
   *   WARNING:  |Z| > 3.0
   *   CRITICAL: |Z| > 4.0
   */
  private def classifySeverity(absZScore: Double): String = {
    if (absZScore > 4.0) "CRITICAL"
    else if (absZScore > 3.0) "WARNING"
    else "INFO"
  }
}

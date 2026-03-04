package com.kaskade

import org.apache.spark.sql.DataFrame

import java.util.Properties

/**
 * PostgreSQL Sink for persistent storage of windowed aggregation results.
 *
 * Writes market metrics to the market_metrics table using JDBC batch inserts.
 * This provides durable storage for historical analysis and Grafana queries
 * that need time-range data beyond the Redis cache TTL.
 *
 * Write strategy: append-only inserts (no upserts) since each window produces
 * unique (symbol, window_start) combinations. The PostgreSQL B-tree index on
 * (symbol, window_start) ensures efficient time-range queries.
 */
object PostgresSink {

  /**
   * Write a batch of windowed aggregation results to PostgreSQL.
   *
   * Uses Spark's built-in JDBC writer with append mode for efficient batch inserts.
   * The DataFrame columns are mapped directly to the market_metrics table columns.
   *
   * @param batchDF      DataFrame with windowed aggregation results
   * @param postgresUrl  JDBC connection URL
   * @param user         PostgreSQL username
   * @param password     PostgreSQL password
   */
  def writeBatch(
    batchDF: DataFrame,
    postgresUrl: String,
    user: String,
    password: String
  ): Unit = {
    val props = new Properties()
    props.setProperty("user", user)
    props.setProperty("password", password)
    props.setProperty("driver", "org.postgresql.Driver")
    // Batch insert size for JDBC efficiency
    props.setProperty("batchsize", "1000")
    // Rewrite batched inserts to multi-row INSERT for PostgreSQL performance
    props.setProperty("reWriteBatchedInserts", "true")

    batchDF.write
      .mode("append")
      .jdbc(postgresUrl, "market_metrics", props)
  }
}

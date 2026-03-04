package com.kaskade

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.avro.functions.from_avro
import org.apache.spark.sql.types._

/**
 * Main entry point for the Spark Structured Streaming application.
 *
 * Orchestrates three concurrent streaming queries:
 *   Query 1 - Window Aggregation: Computes VWAP, volatility, RSI per (symbol, window)
 *   Query 2 - Anomaly Detection: Applies Z-score thresholds to detect market anomalies
 *   Query 3 - Redis State Update: Pushes latest metrics to Redis for real-time dashboards
 *
 * Configuration is loaded from environment variables with defaults for local development.
 */
object StreamingApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Kaskade-StreamProcessor")
      .config("spark.sql.shuffle.partitions", "12")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.streaming.schemaInference", "true")
      .getOrCreate()

    import spark.implicits._

    // Load configuration from environment variables
    val kafkaBootstrap = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092")
    val redisHost = sys.env.getOrElse("REDIS_HOST", "redis")
    val redisPort = sys.env.getOrElse("REDIS_PORT", "6379").toInt
    val postgresHost = sys.env.getOrElse("POSTGRES_HOST", "postgres")
    val postgresPort = sys.env.getOrElse("POSTGRES_PORT", "5432")
    val postgresDb = sys.env.getOrElse("POSTGRES_DB", "financial_analytics")
    val postgresUser = sys.env.getOrElse("POSTGRES_USER", "analytics")
    val postgresPassword = sys.env.getOrElse("POSTGRES_PASSWORD", "analytics_secure_password")
    val checkpointBase = sys.env.getOrElse("CHECKPOINT_DIR", "/opt/spark-checkpoints")

    val postgresUrl = s"jdbc:postgresql://$postgresHost:$postgresPort/$postgresDb"

    println(s"[StreamingApp] Starting with Kafka: $kafkaBootstrap")
    println(s"[StreamingApp] Redis: $redisHost:$redisPort")
    println(s"[StreamingApp] PostgreSQL: $postgresUrl")

    // ================================================================
    // Read raw trade events from Kafka
    // ================================================================
    val rawStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrap)
      .option("subscribe", "raw-trades")
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .option("maxOffsetsPerTrigger", 100000)
      .load()

    // Avro schema for RawTrade (must match producer's Schema Registry schema).
    // The Confluent Schema Registry wire format prepends a 5-byte header
    // (1 magic byte + 4 byte schema ID) before the Avro payload.
    // We strip those 5 bytes, then use spark-avro's from_avro to deserialize.
    val rawTradeAvroSchema =
      """{
        |  "type": "record",
        |  "name": "RawTrade",
        |  "namespace": "com.kaskade.schemas",
        |  "fields": [
        |    {"name": "symbol",    "type": "string"},
        |    {"name": "price",     "type": "double"},
        |    {"name": "volume",    "type": "long"},
        |    {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        |    {"name": "exchange",  "type": "string"},
        |    {"name": "trade_id",  "type": "string"}
        |  ]
        |}""".stripMargin

    // Strip 5-byte Confluent wire-format header, then deserialize Avro binary payload
    val trades = rawStream
      .select(
        col("key").cast("string").as("symbol_key"),
        // Remove the 5-byte Schema Registry header (magic byte + 4-byte schema ID)
        expr("substring(value, 6)").as("avro_value"),
        col("timestamp").as("kafka_timestamp")
      )
      .select(
        from_avro($"avro_value", rawTradeAvroSchema).as("trade"),
        col("kafka_timestamp")
      )
      .select(
        $"trade.symbol".as("symbol"),
        $"trade.price".as("price"),
        $"trade.volume".as("volume"),
        // Use Kafka ingestion timestamp as event time for watermarking
        $"kafka_timestamp".as("event_time"),
        $"trade.exchange".as("exchange"),
        $"trade.trade_id".as("trade_id")
      )
      .filter($"symbol".isNotNull && $"price" > 0)

    // ================================================================
    // Query 1: Window Aggregation (1-minute sliding window, 30s slide)
    // ================================================================
    val windowAgg = WindowAggregation.compute(trades)

    // Write aggregation results to Kafka enriched-events topic
    val enrichedQuery = windowAgg
      .selectExpr("symbol AS key", "to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrap)
      .option("topic", "enriched-events")
      .option("checkpointLocation", s"$checkpointBase/enriched-events")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .outputMode("append")
      .queryName("enriched-events-writer")
      .start()

    // Write aggregation results to PostgreSQL
    val postgresQuery = windowAgg.writeStream
      .foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
        if (!batchDF.isEmpty) {
          PostgresSink.writeBatch(batchDF, postgresUrl, postgresUser, postgresPassword)
        }
      }
      .option("checkpointLocation", s"$checkpointBase/postgres-sink")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .outputMode("append")
      .queryName("postgres-writer")
      .start()

    // ================================================================
    // Query 2: Anomaly Detection
    // ================================================================
    val anomalyQuery = windowAgg.writeStream
      .foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
        if (!batchDF.isEmpty) {
          val alerts = AnomalyDetection.detect(batchDF, redisHost, redisPort)
          if (!alerts.isEmpty) {
            // Write alerts to Kafka alerts topic
            val alertsDF = spark.createDataFrame(
              spark.sparkContext.parallelize(alerts.map(_.toRow)),
              AnomalyDetection.alertSchema
            )
            alertsDF
              .selectExpr("symbol AS key", "to_json(struct(*)) AS value")
              .write
              .format("kafka")
              .option("kafka.bootstrap.servers", kafkaBootstrap)
              .option("topic", "alerts")
              .save()

            println(s"[AnomalyDetection] Batch $batchId: ${alerts.size} alerts generated")
          }
        }
      }
      .option("checkpointLocation", s"$checkpointBase/anomaly-detection")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .outputMode("append")
      .queryName("anomaly-detector")
      .start()

    // ================================================================
    // Query 3: Redis State Update
    // ================================================================
    val redisQuery = windowAgg.writeStream
      .foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
        if (!batchDF.isEmpty) {
          RedisSink.writeBatch(batchDF, redisHost, redisPort)
        }
      }
      .option("checkpointLocation", s"$checkpointBase/redis-sink")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .outputMode("append")
      .queryName("redis-state-updater")
      .start()

    println("[StreamingApp] All streaming queries started. Awaiting termination...")

    // Wait for any query to terminate (or all to finish)
    spark.streams.awaitAnyTermination()
  }
}

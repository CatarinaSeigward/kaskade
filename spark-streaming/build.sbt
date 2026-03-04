// ============================================================
// Kaskade - Spark Structured Streaming Application
// ============================================================
// Builds the streaming pipeline for windowed aggregations,
// anomaly detection, and Redis state updates.
// ============================================================

name := "kaskade-streaming"
version := "1.0.0"
scalaVersion := "2.12.18"

// Spark 3.5 with Structured Streaming
val sparkVersion = "3.5.0"

libraryDependencies ++= Seq(
  // Spark core and streaming
  "org.apache.spark" %% "spark-core"             % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"              % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming"         % sparkVersion % "provided",

  // Kafka connector for Spark Structured Streaming
  "org.apache.spark" %% "spark-sql-kafka-0-10"   % sparkVersion,

  // Avro support for Spark (deserialization of Schema Registry messages)
  "org.apache.spark" %% "spark-avro"             % sparkVersion,

  // Confluent Schema Registry Avro deserializer
  "io.confluent"      % "kafka-schema-registry-client" % "7.6.0",
  "io.confluent"      % "kafka-avro-serializer"        % "7.6.0",
  "org.apache.avro"   % "avro"                         % "1.11.3",

  // Redis client (Jedis) for state management
  "redis.clients"     % "jedis"                        % "5.1.0",

  // PostgreSQL JDBC driver for persistent writes
  "org.postgresql"    % "postgresql"                   % "42.7.1",

  // JSON processing
  "com.google.code.gson" % "gson"                      % "2.10.1",

  // Testing
  "org.scalatest"    %% "scalatest"                    % "3.2.17" % Test,
)

// Confluent Maven repository for Schema Registry dependencies
resolvers += "Confluent" at "https://packages.confluent.io/maven/"

// Assembly plugin configuration for fat JAR
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) =>
    xs match {
      case "MANIFEST.MF" :: Nil => MergeStrategy.discard
      case "services" :: _      => MergeStrategy.concat
      case _                    => MergeStrategy.discard
    }
  case "reference.conf" => MergeStrategy.concat
  case _                => MergeStrategy.first
}

assembly / assemblyJarName := "kaskade-streaming.jar"

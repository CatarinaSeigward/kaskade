FROM bitnami/spark:3.5

USER root

# Copy the pre-built fat JAR (built via sbt assembly)
COPY target/scala-2.12/kaskade-streaming.jar /opt/spark-apps/

# Download required Kafka and Avro connector JARs
RUN curl -L -o /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -L -o /opt/bitnami/spark/jars/kafka-clients-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar && \
    curl -L -o /opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    curl -L -o /opt/bitnami/spark/jars/commons-pool2-2.12.0.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar && \
    curl -L -o /opt/bitnami/spark/jars/postgresql-42.7.1.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar && \
    curl -L -o /opt/bitnami/spark/jars/jedis-5.1.0.jar \
    https://repo1.maven.org/maven2/redis/clients/jedis/5.1.0/jedis-5.1.0.jar

USER 1001

CMD ["spark-submit", \
     "--master", "spark://spark-master:7077", \
     "--class", "com.kaskade.StreamingApp", \
     "--deploy-mode", "client", \
     "--driver-memory", "2g", \
     "--executor-memory", "2g", \
     "/opt/spark-apps/kaskade-streaming.jar"]

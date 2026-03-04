FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for confluent-kafka
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc librdkafka-dev && \
    rm -rf /var/lib/apt/lists/*

COPY producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer/config/ config/
COPY producer/src/ src/
# Avro schemas required by AvroSerializer for Schema Registry integration
COPY schemas/ schemas/

CMD ["python", "-m", "src.main"]

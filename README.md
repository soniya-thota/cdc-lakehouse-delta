# CDC Lakehouse (MySQL → Debezium → Kafka → Spark Δ → FastAPI)

End-to-end demo: MySQL CDC with Debezium → Kafka → Spark Structured Streaming MERGE into Delta Lake, plus FastAPI for time-travel queries.

## Prereqs
- Docker Desktop (Windows/Mac) or Docker + Compose (Linux)
- ~4–6 GB RAM available

## Quick Start
```bash
# clone
git clone https://github.com/soniya-thota/cdc-lakehouse-delta.git
cd cdc-lakehouse-delta

# start stack
docker compose up -d

# register Debezium connector
# PowerShell:
Invoke-RestMethod -Method Post -Uri "http://localhost:8083/connectors" `
  -ContentType "application/json" -InFile "connect/mysql-orders-connector.json"

# install Spark deps (first run only, if needed)
docker compose exec spark pip install -r /app/requirements.txt

# run streaming job
docker compose exec spark bash -lc "/opt/bitnami/spark/bin/spark-submit \
 --packages io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
 --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
 /app/streaming_cdc_to_delta.py"


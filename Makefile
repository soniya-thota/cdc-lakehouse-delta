@'
up:
	docker compose up -d

register:
	curl -i -X POST -H "Accept: application/json" -H "Content-Type: application/json" \
	  --data @connect/mysql-orders-connector.json http://localhost:8083/connectors

submit:
	docker compose exec spark bash -lc '/opt/bitnami/spark/bin/spark-submit \
	  --packages io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
	  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
	  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
	  /app/streaming_cdc_to_delta.py'

down:
	docker compose down -v
'@ | Set-Content -Encoding UTF8 Makefile

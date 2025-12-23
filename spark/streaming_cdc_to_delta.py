import json
from pyspark.sql import SparkSession, functions as F
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

spark = configure_spark_with_delta_pip(
    SparkSession.builder
    .appName("cdc-to-delta")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
).getOrCreate()

KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC = "dbserver1.inventory.orders"
BRONZE = "/data/delta/bronze/orders"
SILVER = "/data/delta/silver/orders"

raw = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load())

val = raw.select(F.col("value").cast("string").alias("json"))

# Bronze: raw CDC envelope
(val.writeStream
 .format("delta")
 .option("checkpointLocation", "/data/checkpoints/bronze_orders")
 .outputMode("append")
 .start(BRONZE))

parsed = val.select(
    F.get_json_object("json", "$.op").alias("op"),
    F.get_json_object("json", "$.after.id").cast("int").alias("id"),
    F.get_json_object("json", "$.after.customer").alias("customer"),
    F.get_json_object("json", "$.after.amount").cast("double").alias("amount"),
    F.get_json_object("json", "$.after.status").alias("status"),
    F.get_json_object("json", "$.ts_ms").cast("timestamp").alias("ts")
)

def upsert_to_delta(microbatch_df, batch_id):
    spark = microbatch_df.sparkSession
    if not DeltaTable.isDeltaTable(spark, SILVER):
        (microbatch_df
         .filter("id IS NOT NULL")
         .withColumn("_deleted", F.lit(False))
         .write.format("delta").mode("overwrite").save(SILVER))
        return

    tgt = DeltaTable.forPath(spark, SILVER)
    updates = (microbatch_df
               .filter("id IS NOT NULL")
               .withColumn("_deleted", (F.col("op") == F.lit("d"))))

    (tgt.alias("t").merge(updates.alias("u"), "t.id = u.id")
        .whenMatchedUpdate(set={
            "customer": "u.customer",
            "amount": "u.amount",
            "status": "u.status",
            "ts": "u.ts",
            "_deleted": "u._deleted"
        })
        .whenNotMatchedInsert(values={
            "id": "u.id",
            "customer": "u.customer",
            "amount": "u.amount",
            "status": "u.status",
            "ts": "u.ts",
            "_deleted": "u._deleted"
        })
        .execute())

(query := parsed.writeStream
    .foreachBatch(upsert_to_delta)
    .option("checkpointLocation", "/data/checkpoints/silver_orders")
    .outputMode("update")
    .start()).awaitTermination()

# spark/streaming_cdc_to_delta.py
# Bronze: store raw CDC events for audit/debug
(val.writeStream
.format("delta")
.option("checkpointLocation", "/data/checkpoints/bronze_orders")
.outputMode("append")
.start(BRONZE))


# Parse columns we care about (before, after, op)
parsed = val.select(
F.get_json_object("json", "$.op").alias("op"),
F.get_json_object("json", "$.after.id").cast("int").alias("id"),
F.get_json_object("json", "$.after.customer").alias("customer"),
F.get_json_object("json", "$.after.amount").cast("double").alias("amount"),
F.get_json_object("json", "$.after.status").alias("status"),
F.get_json_object("json", "$.ts_ms").cast("timestamp").alias("ts")
)


# Upsert logic via foreachBatch + MERGE
from delta.tables import DeltaTable


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

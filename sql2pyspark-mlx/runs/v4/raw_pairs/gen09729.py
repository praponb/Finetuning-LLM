from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("level", "attr_key").orderBy(F.col("latency_ms").desc())
w2 = Window.orderBy("log_ts", "id", "attr_key").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("service").orderBy(F.col("latency_ms").asc_nulls_first())
w4 = Window.partitionBy("attr_key").orderBy("log_ts", "id", "attr_key")
w5 = Window.partitionBy("attr_key").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("level").orderBy("latency_ms_dense_rank", "id", "attr_key")
w_running = Window.partitionBy("attr_key", "level").orderBy("session_id", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("service").orderBy("latency_ms_dense_rank")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

enriched = (
    flattened
    .withColumn("latency_ms_cume_dist", F.cume_dist().over(w_grp))
    .withColumn("latency_ms_avg_2", F.avg("latency_ms").over(w2))
    .withColumn("latency_ms_dense_rank", F.dense_rank().over(w3))
)

gaps = (
    enriched
    .withColumn("is_new_session", F.when(F.lag("log_ts").over(w4).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w4) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessions_app_logs = gaps.withColumn("session_id", F.sum("is_new_session").over(w5))

enriched_app_logs = (
    sessions_app_logs
    .withColumn("prev_latency_ms", F.lag("latency_ms", 1).over(w6))
    .withColumn("last_known_attr_key", F.last("attr_key", ignorenulls=True).over(w_running))
)

deduped = (
    enriched_app_logs
    .withColumn("latest_rank", F.rank().over(w8))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .rollup("level")
    .agg(
        F.min("log_ts").alias("started_at"),
        F.count(F.when(F.col("service") == "billing", 1)).alias("billing_rows"),
        F.grouping_id().alias("grouping_id"),
    )
)

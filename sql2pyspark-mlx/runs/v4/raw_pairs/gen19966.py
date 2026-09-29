from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy("log_ts", "id", "key")
w2 = Window.partitionBy("key").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("key")
w4 = Window.partitionBy("key").orderBy(F.col("latency_ms").desc(), "id", "key")
w5 = Window.orderBy("latency_ms_share", "id", "key")
w6 = Window.partitionBy("key").orderBy("is_new_session", "id", "key")

lines = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

flagged = (
    lines
    .withColumn("is_new_session", F.when(F.col("log_ts") - F.lag("log_ts").over(w) > F.expr("INTERVAL 30 MINUTES"), 1).otherwise(0))
)

islands = flagged.withColumn("island_id", F.sum("is_new_session").over(w2))

with_windows = (
    islands
    .select(
        "*",
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w3)), 4).alias("latency_ms_share"),
        F.ntile(5).over(w4).alias("latency_ms_bucket"),
    )
)

metrics = (
    with_windows
    .withColumn("prev_is_new_session", F.lag("is_new_session", 1, 0).over(w5))
    .withColumn("prev_island_id", F.lag("island_id", 1).over(w6))
)

result = (
    metrics
    .select(
        "id",
        "log_ts",
        "key",
        "v",
        "island_id",
        "latency_ms_share",
        "latency_ms_bucket",
        "prev_is_new_session",
        "prev_island_id",
    )
    .orderBy(F.col("id").desc_nulls_last())
)

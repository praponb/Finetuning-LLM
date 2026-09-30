from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy("log_ts", "id", "key")
w2 = Window.partitionBy("key").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("service").orderBy(F.col("burst_id").desc(), "id", "key")
w4 = Window.partitionBy("host").orderBy("log_ts", "id", "key")
w5 = Window.partitionBy("host").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("service", "key").orderBy(F.col("latency_ms").desc_nulls_first(), "id", "key")
w7 = Window.partitionBy("host").orderBy(F.col("log_ts").desc(), "id", "key")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

gaps = (
    flattened
    .withColumn("new_group_flag", F.when(F.lag("log_ts").over(w).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("new_group_flag").over(w2))

deduped = (
    islands
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

prev_events = deduped.withColumn("prev_log_ts", F.lag("log_ts").over(w4))

with_gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_log_ts").isNull() | (F.col("log_ts") > F.col("prev_log_ts") + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no", F.sum("is_new_session").over(w5))

ranked = (
    sessionized
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
)

latest = (
    ranked
    .withColumn("rank_in_group", F.row_number().over(w7))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        "key",
        "new_group_flag",
        "is_new_session",
    )
)

cleaned = (
    latest
    .filter((F.col("log_ts") > F.to_date(F.lit("2025-06-01"))) & F.col("latency_ms").between(601, 1303))
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "key",
        "new_group_flag",
        F.date_format("log_ts", "MMM yyyy").alias("log_ts_label"),
        F.round((F.col("status_code") * 0.85), 2).alias("status_code_scaled"),
        F.coalesce("key", F.lit("DIRECT")).alias("resolved_key"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "key",
        "new_group_flag",
        "log_ts_label",
    )
)

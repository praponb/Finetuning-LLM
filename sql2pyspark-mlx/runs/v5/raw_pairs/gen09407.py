from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy("log_ts", "id", "key")
w2 = Window.partitionBy("key").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("key").orderBy(F.col("log_ts").desc(), "id", "key")
w4 = Window.partitionBy("key").orderBy(F.col("status_code").desc(), "id", "key")

flattened = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

with_gaps = (
    flattened
    .withColumn("session_start_flag", F.when(F.lag("log_ts").over(w).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

sessions_app_logs = (
    with_gaps
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        "key",
        F.sum("session_start_flag").over(w2).alias("island_id"),
    )
)

ranked = (
    sessions_app_logs
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

ranked_app_logs = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

remaining_keys = (
    ranked_app_logs
    .select("id")
    .intersect(
        df_app_logs
        .filter(F.col("latency_ms") <= 2272)
        .select("id")
    )
)

reconciled = (
    ranked_app_logs
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter(F.col("log_ts") >= F.lit("2025-06-01 09:00:00").cast("timestamp"))
    .select(
        "log_ts",
        "status_code",
        "key",
        "island_id",
        (F.col("status_code") * F.col("latency_ms")).alias("status_code_times_latency_ms"),
        F.coalesce("key", F.lit("UNKNOWN")).alias("resolved_key"),
    )
)

result = (
    valid
    .select("status_code", "island_id", "resolved_key")
    .orderBy(F.col("status_code").desc_nulls_first(), "island_id")
)

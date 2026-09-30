from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("log_ts", "id", "attr_key")
w2 = Window.partitionBy("host").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("attr_key").orderBy("attr_key_length", "id", "attr_key").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("level").orderBy("prev_log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("attr_key").orderBy("prev_log_ts", "id", "attr_key")
w6 = Window.partitionBy("attr_key").orderBy("prev_log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("attr_key", "level").orderBy(F.col("island_id").desc(), "id", "attr_key")
w8 = Window.partitionBy("level").orderBy(F.col("prev_log_ts").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("attr_key", "v"),
    )
)

filtered = (
    lines
    .filter((F.col("log_ts") >= F.lit("2025-06-01 09:00:00").cast("timestamp")) & (~F.col("attr_key").eqNullSafe(F.lit("region"))))
    .select("*", F.length("attr_key").alias("attr_key_length"))
)

with_prev = filtered.withColumn("prev_log_ts", F.lag("log_ts").over(w))

with_gaps = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_log_ts").isNull() | (F.col("log_ts") > F.col("prev_log_ts") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

with_windows = (
    islands
    .filter(F.col("service") == "auth")
    .select(
        "id",
        "level",
        "latency_ms",
        "attr_key",
        "v",
        "prev_log_ts",
        "session_start_flag",
        "island_id",
        F.avg("island_id").over(w3).alias("last3_avg_island_id"),
        F.min("island_id").over(w3).alias("last3_min_island_id"),
        F.count("island_id").over(w3).alias("last3_count_island_id"),
        F.first("latency_ms", ignorenulls=True).over(w4).alias("first_known_latency_ms"),
    )
)

gaps = (
    with_windows
    .withColumn("is_new_burst", F.when(F.lag("prev_log_ts").over(w5).isNull() | (F.col("prev_log_ts") > F.lag("prev_log_ts").over(w5) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands_app_logs = gaps.withColumn("burst_id", F.sum("is_new_burst").over(w6))

enriched = (
    islands_app_logs
    .select(
        "id",
        "level",
        "latency_ms",
        "attr_key",
        "prev_log_ts",
        "island_id",
        "last3_avg_island_id",
        "last3_min_island_id",
        "last3_count_island_id",
        "first_known_latency_ms",
        "is_new_burst",
        "burst_id",
        F.row_number().over(w7).alias("row_seq"),
        F.count("first_known_latency_ms").over(w8).alias("first_known_latency_ms_count_90d"),
    )
)

result = (
    enriched
    .select(
        "id",
        "level",
        "attr_key",
        "last3_count_island_id",
        "is_new_burst",
        "row_seq",
        "first_known_latency_ms_count_90d",
        F.when(F.col("attr_key").rlike("^(trace|version)$"), "GROUP_B").when(F.col("attr_key").rlike("^(region)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("attr_key_zone"),
    )
)

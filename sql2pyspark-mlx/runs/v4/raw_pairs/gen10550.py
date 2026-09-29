from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("log_ts", "id", "k")
w2 = Window.partitionBy("k").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("k").orderBy(F.col("is_new_session").desc_nulls_last())

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

unpacked = (
    flattened
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "k",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

gaps = (
    unpacked
    .withColumn("is_new_burst", F.when(F.col("log_ts") - F.lag("log_ts").over(w) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

islands = gaps.withColumn("session_id", F.sum("is_new_burst").over(w2))

filtered = (
    islands
    .filter((F.col("log_ts") >= F.lit("2025-06-01 17:00:00").cast("timestamp")) & F.col("k").isNotNull())
    .select(
        "id",
        "log_ts",
        "k",
        "has_fault",
        F.round("status_code_high", 0).alias("status_code_high_rounded"),
    )
)

prev_events = filtered.withColumn("prev_log_ts", F.lag("log_ts").over(w))

gaps_2 = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_log_ts").isNull() | (F.col("log_ts") > F.col("prev_log_ts") + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessionized = gaps_2.withColumn("session_no", F.sum("is_new_session").over(w2))

latest = (
    sessionized
    .withColumn("dedup_rank", F.rank().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .groupBy(
        F.date_trunc("YEAR", "prev_log_ts").alias("year_start"),
        "session_no",
        F.when(F.col("status_code_high_rounded") >= 73, "HIGH").otherwise("LOW").alias("status_code_high_rounded_tier"),
    )
    .agg(
        F.percentile_approx("is_new_session", [0.25, 0.5, 0.75]).alias("is_new_session_quartiles"),
        F.count(F.when(F.col("k") == "trace", 1)).alias("trace_count"),
        F.sort_array(F.collect_set("k")).alias("k_list"),
    )
    .orderBy(F.col("year_start").desc_nulls_first())
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("days_since_log_ts").desc())
w2 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w3 = Window.orderBy(F.col("days_since_log_ts").desc_nulls_first())

flattened = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

filtered = (
    flattened
    .filter(F.col("latency_ms").between(852, 1545))
    .select(
        "log_ts",
        "latency_ms",
        F.when(F.col("latency_ms") >= 2000, "High").when(F.col("latency_ms") >= 399, "Medium").otherwise("Low").alias("latency_ms_level"),
        F.round("latency_ms", 2).alias("latency_ms_rounded"),
    )
)

valid = (
    filtered
    .filter(F.col("log_ts") >= F.lit("2025-06-01 11:00:00").cast("timestamp"))
    .select(
        "*",
        F.datediff(F.current_date(), "log_ts").alias("days_since_log_ts"),
        F.floor("latency_ms_rounded").alias("latency_ms_rounded_whole"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.percent_rank().over(w).alias("days_since_log_ts_percent_rank"),
        F.max("latency_ms_rounded").over(w2).alias("latency_ms_rounded_max_2h"),
        F.cume_dist().over(w3).alias("days_since_log_ts_cume_dist"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("latency_ms") >= 1953, "LARGE").otherwise("SMALL").alias("latency_ms_tier"),
    )
    .agg(
        F.percentile_approx("latency_ms_rounded_max_2h", [0.25, 0.5, 0.75]).alias("latency_ms_rounded_max_2h_quartiles"),
        F.min("days_since_log_ts_cume_dist").alias("min_days_since_log_ts_cume_dist"),
        F.percentile_approx("days_since_log_ts", [0.25, 0.5, 0.75]).alias("days_since_log_ts_quartiles"),
    )
    .orderBy("min_days_since_log_ts_cume_dist")
)

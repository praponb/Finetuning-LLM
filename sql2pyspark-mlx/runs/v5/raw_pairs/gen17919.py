from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w2 = Window.partitionBy("level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w_recent = Window.partitionBy("attr_key")
w4 = Window.partitionBy("level", "service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w_recent_2 = Window.partitionBy("service")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

base = (
    flattened
    .filter(F.col("attr_key") != "version")
    .select("*", F.floor("latency_ms").alias("latency_ms_whole"))
)

with_windows = (
    base
    .filter(F.hour("log_ts").between(9, 12))
    .select(
        "*",
        F.sum("latency_ms").over(w).alias("latency_ms_sum_2h"),
        F.count("latency_ms_whole").over(w2).alias("latency_ms_whole_count_15m"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("log_ts") >= F.to_timestamp(F.lit("2025-06-01 13:00:00")))
    .withColumn("greatest_latency_ms_sum_2h_latency_ms_whole", F.greatest("latency_ms_sum_2h", "latency_ms_whole"))
    .withColumn("attr_key_filled", F.coalesce("attr_key", F.lit("N/A")))
)

scored = (
    cleaned
    .select(
        "*",
        F.avg("latency_ms_whole_count_15m").over(w).alias("latency_ms_whole_count_15m_avg_2h"),
        F.avg("greatest_latency_ms_sum_2h_latency_ms_whole").over(w_recent).alias("attr_key_avg_greatest_latency_ms_sum_2h_latency_ms_whole"),
        F.max("latency_ms_whole").over(w_recent).alias("attr_key_max_latency_ms_whole"),
        F.max("latency_ms_whole").over(w4).alias("latency_ms_whole_max_2h"),
        F.max("latency_ms").over(w_recent_2).alias("service_max_latency_ms"),
    )
)

valid = (
    scored
    .filter(F.col("service") == "billing")
    .withColumn("attr_key_avg_greatest_latency_ms_sum_2h_latency_ms_whole_minus_latency_ms_whole_count_15m", (F.col("attr_key_avg_greatest_latency_ms_sum_2h_latency_ms_whole") - F.col("latency_ms_whole_count_15m")))
    .withColumn("attr_key_filled_2", F.coalesce("attr_key", F.lit("DIRECT")))
)

base_app_logs = (
    valid
    .filter(F.col("attr_key").isNotNull() & F.col("latency_ms").between(893, 2061))
    .select(
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "attr_key",
        "attr_value",
        "latency_ms_whole",
        "latency_ms_sum_2h",
        "greatest_latency_ms_sum_2h_latency_ms_whole",
        "attr_key_filled",
        "latency_ms_whole_count_15m_avg_2h",
        "attr_key_avg_greatest_latency_ms_sum_2h_latency_ms_whole",
        "attr_key_max_latency_ms_whole",
        "latency_ms_whole_max_2h",
        "service_max_latency_ms",
        "attr_key_avg_greatest_latency_ms_sum_2h_latency_ms_whole_minus_latency_ms_whole_count_15m",
        "attr_key_filled_2",
        (F.col("latency_ms") + F.col("attr_key_max_latency_ms_whole")).alias("latency_ms_plus_attr_key_max_latency_ms_whole"),
        F.coalesce("attr_key", F.lit("OTHER")).alias("attr_key_clean"),
    )
)

result = (
    base_app_logs
    .groupBy("level")
    .pivot("service", ["checkout", "search"])
    .agg(
        F.sum("attr_key_avg_greatest_latency_ms_sum_2h_latency_ms_whole_minus_latency_ms_whole_count_15m"),
    )
)

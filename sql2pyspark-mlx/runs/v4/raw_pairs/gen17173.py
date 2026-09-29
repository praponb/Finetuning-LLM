from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("host_length")
w2 = Window.partitionBy("host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w3 = Window.partitionBy("host").orderBy(F.col("host_length").asc_nulls_first())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        F.explode_outer("context").alias("k", "v"),
    )
)

cleaned = (
    flattened
    .filter((F.col("log_ts") > F.lit("2025-06-01 06:00:00").cast("timestamp")) & F.col("k").isNotNull())
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "v",
        F.when(F.col("latency_ms") >= 2598, "High").when(F.col("latency_ms") >= 629, "Medium").otherwise("Low").alias("latency_ms_band"),
    )
)

valid = (
    cleaned
    .filter(F.hour("log_ts").between(9, 12))
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "v",
        "latency_ms_band",
        F.when(F.col("latency_ms") >= 2555, "High").when(F.col("latency_ms") >= 372, "Medium").otherwise("Low").alias("latency_ms_level"),
        F.lower("host").alias("host_lower"),
    )
)

filtered = (
    valid
    .filter((F.col("log_ts") >= F.lit("2025-06-01").cast("date")) & F.col("service").isin(["search", "billing"]))
    .select("*", F.length("host").alias("host_length"))
)

with_windows = (
    filtered
    .select(
        "*",
        F.dense_rank().over(w).alias("host_length_dense_rank"),
        F.avg("host_length").over(w2).alias("host_length_avg_15m"),
        F.cume_dist().over(w3).alias("host_length_cume_dist"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "v",
        "latency_ms_level",
        "host_lower",
        "host_length_dense_rank",
        "host_length_avg_15m",
        F.when(F.col("host").rlike("^(h2|h1)$"), "OTHER_ZONE").when(F.col("host").rlike("^(h3|h4)$"), "GROUP_A").otherwise("PRIORITY").alias("host_zone"),
    )
    .orderBy(F.col("id").desc())
)

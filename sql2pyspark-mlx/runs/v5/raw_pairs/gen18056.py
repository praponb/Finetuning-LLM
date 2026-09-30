from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("latency_ms_scaled_2").asc_nulls_first())
w2 = Window.partitionBy("host").orderBy(F.col("latency_ms_scaled").desc())
w3 = Window.partitionBy("host").orderBy(F.col("latency_ms_scaled").asc_nulls_first())
w4 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        F.explode_outer("context").alias("key", "v"),
    )
)

cleaned = (
    lines
    .filter((F.col("log_ts") > F.lit("2025-06-01").cast("date")) & (F.col("latency_ms") > 844))
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "v",
        F.round((F.col("latency_ms") * 1.07), 2).alias("latency_ms_scaled"),
        F.round((F.col("latency_ms") * 100), 2).alias("latency_ms_scaled_2"),
    )
)

valid = (
    cleaned
    .filter(F.col("log_ts") > F.to_timestamp(F.lit("2025-06-01 11:00:00")))
    .select(
        "*",
        F.col("latency_ms").cast("double").alias("latency_ms_str"),
        F.when(F.col("latency_ms") >= 2028, "High").when(F.col("latency_ms") >= 1124, "Medium").otherwise("Low").alias("latency_ms_band"),
    )
)

metrics = (
    valid
    .select(
        "host",
        "latency_ms",
        "v",
        "latency_ms_scaled",
        "latency_ms_scaled_2",
        F.cume_dist().over(w).alias("latency_ms_scaled_2_cume_dist"),
        F.dense_rank().over(w2).alias("latency_ms_scaled_dense_rank"),
        F.cume_dist().over(w3).alias("latency_ms_scaled_cume_dist"),
        F.dense_rank().over(w).alias("latency_ms_scaled_2_dense_rank"),
        F.avg("latency_ms_scaled").over(w4).alias("latency_ms_scaled_avg_30m"),
    )
)

result = (
    metrics
    .select(
        "host",
        "latency_ms",
        "v",
        "latency_ms_scaled",
        "latency_ms_scaled_2_cume_dist",
        "latency_ms_scaled_dense_rank",
        "latency_ms_scaled_cume_dist",
        "latency_ms_scaled_avg_30m",
    )
    .orderBy(F.col("latency_ms_scaled").desc(), "latency_ms_scaled_avg_30m")
)

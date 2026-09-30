from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k", "service").orderBy(F.col("latency_ms").asc_nulls_last())
w2 = Window.partitionBy("k").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

with_windows = flattened.withColumn("latency_ms_cume_dist", F.cume_dist().over(w))

cleaned = (
    with_windows
    .filter((F.col("log_ts") >= F.lit("2025-06-01 06:00:00").cast("timestamp")) & (F.col("latency_ms_cume_dist") < 0.29))
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        "k",
        "attr_value",
        "latency_ms_cume_dist",
        (F.col("latency_ms_cume_dist") - F.col("status_code")).alias("latency_ms_cume_dist_minus_status_code"),
        F.coalesce("k", F.lit("NONE")).alias("k_clean"),
    )
)

enriched = cleaned.withColumn("latency_ms_count_1h", F.count("latency_ms").over(w2))

result = (
    enriched
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        "k",
        "attr_value",
        "latency_ms_cume_dist",
        "latency_ms_cume_dist_minus_status_code",
        "k_clean",
        F.when(F.col("k").rlike("^(trace|version)$"), "CORE").when(F.col("k").rlike("^(region)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("k_zone"),
    )
    .orderBy(F.col("k").desc_nulls_first())
)

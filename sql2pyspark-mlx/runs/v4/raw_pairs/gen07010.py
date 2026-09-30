from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w2 = Window.partitionBy("k")
w3 = Window.partitionBy("k").orderBy(F.col("region_events").desc())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

enriched = (
    flattened
    .select(
        "*",
        F.max("latency_ms").over(w).alias("latency_ms_max_15m"),
        F.corr("status_code", "latency_ms").over(w2).alias("corr_status_code_latency_ms"),
    )
)

cleaned = (
    enriched
    .filter((F.col("log_ts") > F.lit("2025-06-01 06:00:00").cast("timestamp")) & F.col("service").isin(["auth", "checkout"]))
    .select(
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        "k",
        "value",
        "latency_ms_max_15m",
        "corr_status_code_latency_ms",
        F.trim("level").alias("clean_level"),
    )
)

result = (
    cleaned
    .groupBy(
        "k",
        F.when(F.col("corr_status_code_latency_ms") >= 0.62, "HIGH").otherwise("LOW").alias("corr_status_code_latency_ms_tier"),
    )
    .agg(
        F.percentile_approx("status_code", [0.25, 0.5, 0.75]).alias("status_code_quartiles"),
        F.size(F.collect_set("level")).alias("num_levels"),
        F.count_if(F.col("k") == "region").alias("region_events"),
        F.percentile_approx("latency_ms_max_15m", [0.25, 0.5, 0.75]).alias("latency_ms_max_15m_quartiles"),
    )
    .filter(F.col("num_levels") < 1)
    .withColumn("region_events_dense_rank", F.dense_rank().over(w3))
)

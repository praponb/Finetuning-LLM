from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key")
w2 = Window.orderBy(F.col("latency_ms").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

with_windows = lines.withColumn("attr_key_avg_latency_ms", F.avg("latency_ms").over(w))

cleaned = (
    with_windows
    .filter(F.col("host") == "h3")
    .select(
        "*",
        (F.col("latency_ms") + F.col("status_code")).alias("latency_ms_plus_status_code"),
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "attr_key",
        "value",
        "latency_ms_plus_status_code",
        F.corr("latency_ms", "status_code").over(w).alias("corr_latency_ms_status_code"),
        F.rank().over(w2).alias("latency_ms_rank"),
    )
)

valid = (
    metrics
    .filter(F.col("attr_key").isNotNull() & F.col("corr_latency_ms_status_code").between(-0.5, -0.3))
    .select(
        "*",
        (F.col("corr_latency_ms_status_code") * F.col("latency_ms_plus_status_code")).alias("corr_latency_ms_status_code_times_latency_ms_plus_status_code"),
        F.round("latency_ms_plus_status_code", 0).alias("latency_ms_plus_status_code_rounded"),
        F.coalesce("attr_key", F.lit("DIRECT")).alias("attr_key_clean"),
    )
)

result = (
    valid
    .select(
        "id",
        "attr_key",
        "latency_ms_plus_status_code",
        "corr_latency_ms_status_code",
        "latency_ms_rank",
        "corr_latency_ms_status_code_times_latency_ms_plus_status_code",
    )
    .orderBy(F.col("attr_key").desc_nulls_last())
)

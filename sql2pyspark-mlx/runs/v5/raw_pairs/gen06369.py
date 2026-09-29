from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("service").orderBy(F.col("status_code_rounded").desc_nulls_first())
w_running = Window.partitionBy("host")
w3 = Window.partitionBy("service")
w4 = Window.orderBy("status_code_rounded")
w5 = Window.orderBy(F.col("corr_latency_ms_status_code").desc_nulls_last())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

kept_keys = (
    lines
    .select("service", "id")
    .subtract(
        df_app_logs
        .filter(F.col("service") == "checkout")
        .select("service", "id")
    )
)

kept_rows = (
    lines
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.service") == F.col("k.service")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter(F.col("status_code") == 282)
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        "attr_value",
        F.round("status_code", 2).alias("status_code_rounded"),
    )
)

metrics = (
    valid
    .filter(F.col("service") != "search")
    .select(
        "id",
        "attr_value",
        "status_code_rounded",
        F.percent_rank().over(w_recent).alias("status_code_rounded_percent_rank"),
        F.avg("latency_ms").over(w_running).alias("host_avg_latency_ms"),
        F.corr("latency_ms", "status_code").over(w3).alias("corr_latency_ms_status_code"),
    )
)

with_windows = metrics.withColumn("status_code_rounded_rank", F.rank().over(w4))

filtered = (
    with_windows
    .filter(F.col("host_avg_latency_ms") > 2108)
    .select(
        "id",
        "attr_value",
        "status_code_rounded",
        "status_code_rounded_percent_rank",
        "host_avg_latency_ms",
        "corr_latency_ms_status_code",
        F.col("status_code_rounded").cast("int").alias("status_code_rounded_int"),
    )
)

with_windows_app_logs = filtered.select("*", F.rank().over(w5).alias("corr_latency_ms_status_code_rank"))

result = (
    with_windows_app_logs
    .groupBy(
        F.when(F.col("corr_latency_ms_status_code") >= 0.8, "LARGE").otherwise("SMALL").alias("corr_latency_ms_status_code_tier"),
    )
    .agg(
        F.percentile_approx("status_code_rounded_percent_rank", 0.95).alias("status_code_rounded_percent_rank_p95"),
        F.min("host_avg_latency_ms").alias("min_host_avg_latency_ms"),
        F.percentile_approx("status_code_rounded_int", 0.9).alias("status_code_rounded_int_p90"),
    )
)

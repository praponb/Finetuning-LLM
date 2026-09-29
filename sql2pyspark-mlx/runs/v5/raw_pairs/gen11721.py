from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("k").orderBy("greatest_status_code_latency_ms", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("level").orderBy("running_sum_greatest_status_code_latency_ms", "id", "k").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("level").orderBy(F.col("last4_sum_running_sum_greatest_status_code_latency_ms").asc_nulls_last())
w4 = Window.partitionBy("level")
w5 = Window.partitionBy("k")

lines = (
    df_app_logs
    .select(
        "id",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

filtered = (
    lines
    .filter(F.col("k").isNotNull())
    .select(
        "*",
        F.greatest("status_code", "latency_ms").alias("greatest_status_code_latency_ms"),
        F.coalesce("k", F.lit("NONE")).alias("k_filled"),
    )
)

with_windows = (
    filtered
    .select(
        "id",
        "level",
        "status_code",
        "k",
        "attr_value",
        F.sum("greatest_status_code_latency_ms").over(w_grp).alias("running_sum_greatest_status_code_latency_ms"),
        F.avg("greatest_status_code_latency_ms").over(w_grp).alias("running_avg_greatest_status_code_latency_ms"),
        F.min("greatest_status_code_latency_ms").over(w_grp).alias("running_min_greatest_status_code_latency_ms"),
    )
)

scored = (
    with_windows
    .filter(F.col("running_avg_greatest_status_code_latency_ms").between(31, 53))
    .select(
        "*",
        F.sum("running_sum_greatest_status_code_latency_ms").over(w2).alias("last4_sum_running_sum_greatest_status_code_latency_ms"),
        F.avg("running_sum_greatest_status_code_latency_ms").over(w2).alias("last4_avg_running_sum_greatest_status_code_latency_ms"),
        F.count("running_sum_greatest_status_code_latency_ms").over(w2).alias("last4_count_running_sum_greatest_status_code_latency_ms"),
    )
)

with_windows_app_logs = (
    scored
    .select(
        "*",
        F.percent_rank().over(w3).alias("last4_sum_running_sum_greatest_status_code_latency_ms_percent_rank"),
        F.count("running_avg_greatest_status_code_latency_ms").over(w4).alias("level_count_running_avg_greatest_status_code_latency_ms"),
        F.round((F.col("last4_count_running_sum_greatest_status_code_latency_ms") / F.sum("last4_count_running_sum_greatest_status_code_latency_ms").over(w5)), 4).alias("last4_count_running_sum_greatest_status_code_latency_ms_share"),
    )
)

result = (
    with_windows_app_logs
    .groupBy("level")
    .agg(
        F.count(F.when(F.col("k") == "version", 1)).alias("version_count"),
        F.max(F.when(F.col("level") == "DEBUG", F.col("last4_sum_running_sum_greatest_status_code_latency_ms_percent_rank"))).alias("debug_max_last4_sum_running_sum_greatest_status_code_latency_ms_percent_rank"),
        F.percentile_approx("last4_avg_running_sum_greatest_status_code_latency_ms", 0.9).alias("last4_avg_running_sum_greatest_status_code_latency_ms_p90"),
    )
    .filter(F.col("debug_max_last4_sum_running_sum_greatest_status_code_latency_ms_percent_rank") > 0)
)

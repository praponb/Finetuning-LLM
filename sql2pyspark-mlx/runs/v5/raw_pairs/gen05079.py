from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host", "level").orderBy("status_code", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("status_code")
w_grp = Window.partitionBy("level").orderBy("cumulative_latency_ms", "id", "attr_key").rowsBetween(-2, -1)
w4 = Window.partitionBy("host")
w_ordered = Window.partitionBy("host").orderBy("host_stddev_samp_status_code_cume_dist", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

enriched = (
    flattened
    .select(
        "*",
        F.sum("latency_ms").over(w).alias("cumulative_latency_ms"),
        F.cume_dist().over(w2).alias("status_code_cume_dist"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "host",
        "latency_ms",
        "attr_key",
        "status_code_cume_dist",
        F.avg("cumulative_latency_ms").over(w_grp).alias("cumulative_latency_ms_avg_2_prior"),
        F.stddev_samp("status_code_cume_dist").over(w4).alias("host_stddev_samp_status_code_cume_dist"),
    )
)

scored = (
    with_windows
    .select(
        "*",
        F.avg("host_stddev_samp_status_code_cume_dist").over(w_ordered).alias("running_avg_host_stddev_samp_status_code_cume_dist"),
        F.count("host_stddev_samp_status_code_cume_dist").over(w_ordered).alias("running_count_host_stddev_samp_status_code_cume_dist"),
        F.sum("host_stddev_samp_status_code_cume_dist").over(w_ordered).alias("running_sum_host_stddev_samp_status_code_cume_dist"),
        F.min("host_stddev_samp_status_code_cume_dist").over(w_ordered).alias("running_min_host_stddev_samp_status_code_cume_dist"),
    )
)

valid = (
    scored
    .filter(F.col("running_avg_host_stddev_samp_status_code_cume_dist") > 0.61)
    .select(
        "id",
        "latency_ms",
        "attr_key",
        "status_code_cume_dist",
        "host_stddev_samp_status_code_cume_dist",
        "running_avg_host_stddev_samp_status_code_cume_dist",
        "running_sum_host_stddev_samp_status_code_cume_dist",
        "running_min_host_stddev_samp_status_code_cume_dist",
        F.ceil("cumulative_latency_ms_avg_2_prior").alias("cumulative_latency_ms_avg_2_prior_whole"),
        F.coalesce("attr_key", F.lit("DIRECT")).alias("attr_key_filled"),
    )
)

result = (
    valid
    .select(
        "id",
        "latency_ms",
        "attr_key",
        "host_stddev_samp_status_code_cume_dist",
        "running_avg_host_stddev_samp_status_code_cume_dist",
        "running_sum_host_stddev_samp_status_code_cume_dist",
        "running_min_host_stddev_samp_status_code_cume_dist",
        "cumulative_latency_ms_avg_2_prior_whole",
        F.when(F.col("attr_key").rlike("^(trace|version)$"), "PRIORITY").otherwise("GROUP_B").alias("attr_key_zone"),
    )
    .orderBy(F.col("running_avg_host_stddev_samp_status_code_cume_dist").desc())
)

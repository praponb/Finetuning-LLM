from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("status_code").desc())
w2 = Window.partitionBy("key")
w_hist = Window.partitionBy("service", "host")
w4 = Window.partitionBy("key").orderBy(F.col("status_code_cume_dist").desc_nulls_first())
w5 = Window.partitionBy("service")
w6 = Window.orderBy("status_code")
w_recent = Window.partitionBy("level").orderBy(F.col("service_count_status_code_cume_dist").desc())
w8 = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w_part = Window.partitionBy("key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)

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
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

metrics = (
    flattened
    .select(
        "*",
        F.cume_dist().over(w).alias("status_code_cume_dist"),
        F.sum("status_code").over(w2).alias("key_sum_status_code"),
    )
)

with_windows = (
    metrics
    .withColumn("corr_latency_ms_status_code", F.corr("latency_ms", "status_code").over(w_hist))
    .withColumn("status_code_cume_dist_percent_rank", F.percent_rank().over(w4))
    .withColumn("service_count_status_code_cume_dist", F.count("status_code_cume_dist").over(w5))
    .withColumn("status_code_rank", F.rank().over(w6))
)

filtered = (
    with_windows
    .filter((F.col("log_ts") >= F.to_timestamp(F.lit("2025-06-01 15:00:00"))) & F.col("key").isNotNull())
    .select("*", F.trim("service").alias("clean_service"))
)

cleaned = (
    filtered
    .filter(F.col("level") == "INFO")
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "key",
        "attr_value",
        "status_code_cume_dist",
        "key_sum_status_code",
        "corr_latency_ms_status_code",
        "status_code_cume_dist_percent_rank",
        "service_count_status_code_cume_dist",
        "clean_service",
        F.concat("level", F.lit(", "), "host").alias("level_host"),
    )
)

metrics_app_logs = (
    cleaned
    .withColumn("status_code_percent_rank", F.percent_rank().over(w6))
    .withColumn("service_count_status_code_cume_dist_cume_dist", F.cume_dist().over(w_recent))
    .withColumn("corr_latency_ms_status_code_2", F.corr("latency_ms", "status_code").over(w5))
)

with_windows_app_logs = (
    metrics_app_logs
    .select(
        "*",
        F.avg("latency_ms").over(w8).alias("latency_ms_avg_1h"),
        F.max("service_count_status_code_cume_dist_cume_dist").over(w_part).alias("service_count_status_code_cume_dist_cume_dist_max_2h"),
    )
)

result = (
    with_windows_app_logs
    .groupBy(F.date_trunc("day", "log_ts").alias("activity_day"))
    .agg(
        F.percentile_approx("service_count_status_code_cume_dist_cume_dist_max_2h", 0.9).alias("service_count_status_code_cume_dist_cume_dist_max_2h_p90"),
        F.round(F.sum("corr_latency_ms_status_code_2") / F.nullif(F.count(F.when(F.col("host") == "h1", 1)), F.lit(0)), 2).alias("corr_latency_ms_status_code_2_per_h1"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("duration_min"),
    )
    .filter(F.col("corr_latency_ms_status_code_2_per_h1") > 0)
)

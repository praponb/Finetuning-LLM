from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host", "level")
w2 = Window.partitionBy("level")
w3 = Window.partitionBy("level").orderBy(F.col("status_code").desc())
w_grp = Window.partitionBy("service", "host").orderBy("host_min_status_code")
w5 = Window.partitionBy("service")
w_ordered = Window.partitionBy("service").orderBy("status_code")
w7 = Window.partitionBy("host")
w8 = Window.partitionBy("host_min_status_code_rank_tier").orderBy(F.col("status_code_share_p90").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

with_windows = (
    lines
    .select(
        "service",
        "host",
        "status_code",
        "v",
        F.min("status_code").over(w).alias("host_min_status_code"),
        F.round((F.col("status_code") / F.sum("status_code").over(w2)), 4).alias("status_code_share"),
        F.rank().over(w3).alias("status_code_rank"),
    )
)

enriched = (
    with_windows
    .filter(F.col("status_code_rank") >= 7)
    .select(
        "*",
        F.rank().over(w_grp).alias("host_min_status_code_rank"),
        F.stddev_samp("host_min_status_code").over(w5).alias("service_stddev_samp_host_min_status_code"),
        F.rank().over(w_ordered).alias("status_code_rank_2"),
        F.rank().over(w_ordered).alias("status_code_rank_3"),
    )
)

enriched_app_logs = enriched.withColumn("host_count_status_code", F.count("status_code").over(w7))

base = (
    enriched_app_logs
    .filter(F.col("host_count_status_code") <= 15)
    .withColumn("is_search", F.when(F.col("service") == "search", 1).otherwise(0))
)

result = (
    base
    .groupBy(
        F.when(F.col("host_min_status_code_rank") >= 8, "HIGH").otherwise("LOW").alias("host_min_status_code_rank_tier"),
        "service",
    )
    .agg(
        F.percentile_approx("status_code_share", 0.9).alias("status_code_share_p90"),
        F.round(F.avg("service_stddev_samp_host_min_status_code"), 2).alias("avg_service_stddev_samp_host_min_status_code"),
        F.countDistinct("service").alias("distinct_services"),
        F.round(F.avg("service_stddev_samp_host_min_status_code"), 2).alias("avg_service_stddev_samp_host_min_status_code_2"),
        F.percentile_approx("status_code_share", 0.9).alias("status_code_share_p90_2"),
        F.round(F.sum("service_stddev_samp_host_min_status_code") / F.nullif(F.count(F.when(F.col("service") == "billing", 1)), F.lit(0)), 2).alias("service_stddev_samp_host_min_status_code_per_billing"),
    )
    .withColumn("status_code_share_p90_rank", F.rank().over(w8))
)

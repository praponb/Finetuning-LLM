from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w_recent = Window.orderBy(F.col("status_code").desc())
w_part = Window.partitionBy("service", "key")
w4 = Window.orderBy("status_code_percent_rank")
w5 = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w6 = Window.partitionBy("service")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

scored = (
    lines
    .select(
        "*",
        F.avg("status_code").over(w_grp).alias("status_code_avg_1h"),
        F.percent_rank().over(w_recent).alias("status_code_percent_rank"),
        F.max("status_code").over(w_part).alias("service_max_status_code"),
    )
)

base = (
    scored
    .filter(F.col("status_code_avg_1h").between(263, 382))
    .select(
        "service",
        "log_ts",
        "value",
        "status_code_avg_1h",
        "status_code_percent_rank",
        "service_max_status_code",
        F.round((F.col("status_code") / F.col("service_max_status_code")), 2).alias("status_code_per_service_max_status_code"),
        F.coalesce("key", F.lit("N/A")).alias("resolved_key"),
    )
)

metrics = (
    base
    .select(
        "*",
        F.percent_rank().over(w4).alias("status_code_percent_rank_percent_rank"),
        F.sum("status_code_avg_1h").over(w5).alias("status_code_avg_1h_sum_15m"),
        F.stddev_samp("status_code_percent_rank").over(w6).alias("service_stddev_samp_status_code_percent_rank"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("status_code_percent_rank_percent_rank") >= 0.66, "PREMIUM").otherwise("STANDARD").alias("status_code_percent_rank_percent_rank_tier"),
    )
    .agg(
        F.count_if(F.col("service") == "checkout").alias("checkout_events"),
        F.count(F.when(F.col("service") == "auth", 1)).alias("auth_count"),
        F.avg("status_code_per_service_max_status_code").alias("avg_status_code_per_service_max_status_code"),
        F.countDistinct("service").alias("distinct_services"),
    )
    .filter(F.col("avg_status_code_per_service_max_status_code") > 0)
)

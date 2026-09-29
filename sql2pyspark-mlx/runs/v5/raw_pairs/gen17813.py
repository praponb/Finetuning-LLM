from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w2 = Window.orderBy(F.col("status_code").desc())
w_grp = Window.partitionBy("level").orderBy("status_code")
w4 = Window.partitionBy("level", "k")
w5 = Window.partitionBy("level").orderBy(F.col("level_count_status_code_percent_rank").desc_nulls_last())
w_hist = Window.partitionBy("level").orderBy(F.col("status_code_percent_rank").asc_nulls_last())
w7 = Window.orderBy("level_count_level_count_status_code_percent_rank")

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

valid = (
    lines
    .filter(F.col("log_ts") >= F.lit("2025-06-01 05:00:00").cast("timestamp"))
    .select(
        "id",
        "level",
        "status_code",
        "k",
        F.concat_ws("-", "k", "level").alias("k_level_key"),
        F.upper("host").alias("host_upper"),
        F.coalesce("k", F.lit("UNKNOWN")).alias("k_clean"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "level",
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share"),
        F.sum("status_code").over(w).alias("level_sum_status_code"),
        F.rank().over(w2).alias("status_code_rank"),
        F.percent_rank().over(w_grp).alias("status_code_percent_rank"),
        F.max("status_code").over(w4).alias("level_max_status_code"),
    )
)

metrics_app_logs = (
    metrics
    .filter(F.col("status_code_share").between(0.36, 0.77))
    .select(
        "id",
        "level",
        "status_code_share",
        "level_sum_status_code",
        "status_code_percent_rank",
        "level_max_status_code",
        F.round((F.col("level_max_status_code") / F.sum("level_max_status_code").over(w)), 4).alias("level_max_status_code_share"),
        F.count("status_code_percent_rank").over(w).alias("level_count_status_code_percent_rank"),
    )
)

metrics_app_logs_2 = (
    metrics_app_logs
    .select(
        "*",
        F.count("level_count_status_code_percent_rank").over(w).alias("level_count_level_count_status_code_percent_rank"),
        F.percent_rank().over(w5).alias("level_count_status_code_percent_rank_percent_rank"),
        F.percent_rank().over(w_hist).alias("status_code_percent_rank_percent_rank"),
    )
)

scored = (
    metrics_app_logs_2
    .select(
        "id",
        "status_code_share",
        "status_code_percent_rank",
        "level_max_status_code",
        "level_max_status_code_share",
        "level_count_status_code_percent_rank",
        "level_count_level_count_status_code_percent_rank",
        F.sum("status_code_share").over(w).alias("level_sum_status_code_share"),
        F.percent_rank().over(w7).alias("level_count_level_count_status_code_percent_rank_percent_rank"),
    )
)

filtered = (
    scored
    .filter(F.col("status_code_percent_rank") >= 0.69)
    .withColumn("level_count_status_code_percent_rank_per_level_max_status_code_share", (F.col("level_count_status_code_percent_rank") / F.nullif("level_max_status_code_share", F.lit(0))))
)

result = (
    filtered
    .select(
        "id",
        "status_code_percent_rank",
        "level_max_status_code_share",
        "level_count_status_code_percent_rank",
        "level_count_level_count_status_code_percent_rank",
        "level_sum_status_code_share",
        "level_count_level_count_status_code_percent_rank_percent_rank",
    )
    .orderBy(F.col("status_code_percent_rank").desc_nulls_last())
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host", "level")
w2 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w3 = Window.partitionBy("host").orderBy(F.col("status_code_share").desc_nulls_last())
w4 = Window.partitionBy("host", "level").orderBy(F.col("status_code_share_filled").asc_nulls_first())

lines = (
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
    lines
    .select(
        "*",
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share"),
        F.count("status_code").over(w2).alias("status_code_count_2h"),
    )
)

valid = (
    enriched
    .filter(F.col("log_ts") > F.lit("2025-06-01").cast("date"))
    .select(
        "host",
        "log_ts",
        "level",
        "k",
        "value",
        "status_code_share",
        F.upper("k").alias("k_upper"),
        F.concat("service", F.lit(", "), "level").alias("service_level"),
    )
)

cleaned = (
    valid
    .filter(F.col("k").isNotNull())
    .select(
        "host",
        "log_ts",
        "level",
        "value",
        "status_code_share",
        "k_upper",
        "service_level",
        F.coalesce("status_code_share", F.lit(0)).alias("status_code_share_filled"),
    )
)

result = (
    cleaned
    .select(
        "*",
        F.dense_rank().over(w3).alias("status_code_share_dense_rank"),
        F.stddev_samp("status_code_share_filled").over(w).alias("host_stddev_samp_status_code_share_filled"),
        F.rank().over(w4).alias("status_code_share_filled_rank"),
    )
)

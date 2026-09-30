from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "attr_key").orderBy("status_code_high")
w2 = Window.partitionBy("attr_key").orderBy(F.col("status_code_high").desc_nulls_first())
w3 = Window.orderBy(F.col("status_code_high").asc_nulls_last())
w4 = Window.partitionBy("level", "attr_key")
w5 = Window.partitionBy("attr_key")

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
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

parsed = (
    flattened
    .select(
        "id",
        "log_ts",
        "level",
        "attr_key",
        "attr_value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
    )
)

with_windows = (
    parsed
    .select(
        "level",
        "status_code_high",
        F.rank().over(w).alias("status_code_high_rank"),
        F.percent_rank().over(w2).alias("status_code_high_percent_rank"),
        F.percent_rank().over(w3).alias("status_code_high_percent_rank_2"),
        F.stddev_samp("has_fault").over(w4).alias("level_stddev_samp_has_fault"),
        F.min("status_code_high").over(w5).alias("attr_key_min_status_code_high"),
    )
)

valid = (
    with_windows
    .filter(F.col("status_code_high_rank") > 7)
    .withColumn("status_code_high_adjusted", F.round((F.col("status_code_high") * 0.85), 2))
)

cleaned = (
    valid
    .filter(F.col("level") == "DEBUG")
    .select(
        "*",
        F.ceil("status_code_high_percent_rank").alias("status_code_high_percent_rank_whole"),
    )
)

totals = (
    cleaned
    .groupBy("level")
    .agg(
        F.sum("status_code_high_percent_rank_2").alias("total_status_code_high_percent_rank_2"),
        F.percentile_approx("attr_key_min_status_code_high", [0.25, 0.5, 0.75]).alias("attr_key_min_status_code_high_quartiles"),
    )
)

result = (
    totals
    .select(
        "level",
        "total_status_code_high_percent_rank_2",
        "attr_key_min_status_code_high_quartiles",
    )
)

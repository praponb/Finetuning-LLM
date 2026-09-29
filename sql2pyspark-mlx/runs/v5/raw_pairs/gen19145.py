from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("is_flagged").asc_nulls_last())
w_recent = Window.orderBy(F.col("status_code_high").desc_nulls_first())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

unpacked = (
    exploded
    .select(
        "id",
        "status_code",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

with_windows = (
    unpacked
    .select(
        "status_code",
        "is_flagged",
        "status_code_high",
        F.percent_rank().over(w).alias("is_flagged_percent_rank"),
        F.dense_rank().over(w_recent).alias("status_code_high_dense_rank"),
    )
)

filtered = (
    with_windows
    .filter(F.col("status_code_high_dense_rank") != 5)
    .select(
        "status_code",
        "is_flagged",
        "status_code_high_dense_rank",
        F.greatest("status_code", "is_flagged_percent_rank").alias("greatest_status_code_is_flagged_percent_rank"),
    )
)

cleaned = (
    filtered
    .filter(F.col("status_code_high_dense_rank") <= 8)
    .withColumn("is_flagged_per_status_code_high_dense_rank", F.round((F.col("is_flagged") / F.col("status_code_high_dense_rank")), 2))
)

result = (
    cleaned
    .withColumn("is_flagged_tier", F.when(F.col("is_flagged") >= 1, "PREMIUM").otherwise("STANDARD"))
    .withColumn("greatest_status_code_is_flagged_percent_rank_tier", F.when(F.col("greatest_status_code_is_flagged_percent_rank") >= 67, "PREMIUM").otherwise("STANDARD"))
    .cube("is_flagged_tier", "greatest_status_code_is_flagged_percent_rank_tier")
    .agg(
        F.round(F.avg("is_flagged"), 2).alias("avg_is_flagged"),
        F.percentile_approx("is_flagged_per_status_code_high_dense_rank", 0.5).alias("is_flagged_per_status_code_high_dense_rank_p50"),
        F.min("is_flagged_per_status_code_high_dense_rank").alias("min_is_flagged_per_status_code_high_dense_rank"),
        F.round(F.avg("greatest_status_code_is_flagged_percent_rank"), 2).alias("avg_greatest_status_code_is_flagged_percent_rank"),
        F.count("*").alias("event_count"),
        F.percentile_approx("is_flagged_per_status_code_high_dense_rank", [0.25, 0.5, 0.75]).alias("is_flagged_per_status_code_high_dense_rank_quartiles"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("avg_is_flagged") > 0)
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("days_since_log_ts").desc_nulls_last())
w2 = Window.orderBy(F.col("days_since_log_ts").asc_nulls_last())
w3 = Window.orderBy(F.col("log_ts_day_period").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w4 = Window.orderBy(F.col("log_ts_day_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)

exploded = (
    df_app_logs
    .select("id", "log_ts", F.explode_outer("context").alias("attr_key", "v"))
)

base = (
    exploded
    .filter(F.col("attr_key").isNotNull())
    .select(
        "*",
        F.col("log_ts").cast("date").alias("log_ts_day"),
        F.datediff(F.current_date(), "log_ts").alias("days_since_log_ts"),
    )
)

valid = (
    base
    .filter(F.col("attr_key").isNotNull())
    .select(
        "id",
        "log_ts",
        "days_since_log_ts",
        F.date_trunc("month", "log_ts_day").alias("log_ts_day_period"),
    )
)

enriched = (
    valid
    .filter(F.col("log_ts") < "2024-08-01")
    .select(
        "*",
        F.cume_dist().over(w).alias("days_since_log_ts_cume_dist"),
        F.cume_dist().over(w2).alias("days_since_log_ts_cume_dist_2"),
    )
)

valid_app_logs = (
    enriched
    .filter((F.col("log_ts") >= F.to_date(F.lit("2025-06-01"))) & (F.col("days_since_log_ts_cume_dist_2") <= 0.64))
    .select(
        "id",
        "days_since_log_ts",
        "log_ts_day_period",
        F.round("days_since_log_ts_cume_dist_2", 1).alias("days_since_log_ts_cume_dist_2_rounded"),
    )
)

with_windows = (
    valid_app_logs
    .select(
        "*",
        F.max("days_since_log_ts_cume_dist_2_rounded").over(w3).alias("days_since_log_ts_cume_dist_2_rounded_max_90d"),
        F.sum("days_since_log_ts").over(w3).alias("days_since_log_ts_sum_90d"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "days_since_log_ts",
        F.avg("days_since_log_ts_cume_dist_2_rounded").over(w4).alias("days_since_log_ts_cume_dist_2_rounded_avg_7d"),
    )
)

grouped = (
    metrics
    .withColumn("days_since_log_ts_cume_dist_2_rounded_avg_7d_tier", F.when(F.col("days_since_log_ts_cume_dist_2_rounded_avg_7d") >= 88, "PREMIUM").otherwise("STANDARD"))
    .withColumn("days_since_log_ts_tier", F.when(F.col("days_since_log_ts") >= 77, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["days_since_log_ts_cume_dist_2_rounded_avg_7d_tier", "days_since_log_ts_tier"], ["days_since_log_ts_cume_dist_2_rounded_avg_7d_tier"]],
        "days_since_log_ts_cume_dist_2_rounded_avg_7d_tier",
        "days_since_log_ts_tier",
    )
    .agg(
        F.sum("days_since_log_ts").alias("total_days_since_log_ts"),
        F.max("days_since_log_ts_cume_dist_2_rounded_avg_7d").alias("max_days_since_log_ts_cume_dist_2_rounded_avg_7d"),
        F.max("days_since_log_ts").alias("max_days_since_log_ts"),
        F.grouping("days_since_log_ts_cume_dist_2_rounded_avg_7d_tier").alias("grouping_id"),
    )
)

result = (
    grouped
    .select(
        "days_since_log_ts_cume_dist_2_rounded_avg_7d_tier",
        "days_since_log_ts_tier",
        "total_days_since_log_ts",
        "max_days_since_log_ts_cume_dist_2_rounded_avg_7d",
        "max_days_since_log_ts",
    )
)

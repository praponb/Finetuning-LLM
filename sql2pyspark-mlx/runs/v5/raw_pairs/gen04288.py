from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key", "level")
w2 = Window.partitionBy("attr_key", "level").orderBy("status_code")
w3 = Window.partitionBy("attr_key", "level").orderBy("status_code", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("attr_key")
w5 = Window.partitionBy("level").orderBy("status_code_percent_rank", "id", "attr_key").rowsBetween(-3, Window.currentRow)
w6 = Window.orderBy("status_code_share", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("attr_key", "level").orderBy("status_code_share_avg_3", "id", "attr_key")
w_recent = Window.partitionBy("attr_key", "level").orderBy("status_code_percent_rank", "id", "attr_key").rowsBetween(-2, Window.currentRow)
w9 = Window.partitionBy("attr_key").orderBy("status_code_share_avg_2", "id", "attr_key")

lines = (
    df_app_logs
    .select(
        "id",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

with_windows = (
    lines
    .select(
        "*",
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share"),
        F.percent_rank().over(w2).alias("status_code_percent_rank"),
        F.first("attr_key", ignorenulls=True).over(w3).alias("first_known_attr_key"),
        F.stddev_samp("status_code").over(w4).alias("attr_key_stddev_samp_status_code"),
    )
)

metrics = (
    with_windows
    .filter(F.col("status_code_percent_rank") > 0.47)
    .withColumn("status_code_share_avg_3", F.avg("status_code_share").over(w5))
    .withColumn("cumulative_status_code_percent_rank", F.sum("status_code_percent_rank").over(w6))
)

enriched = (
    metrics
    .select(
        "id",
        "level",
        "attr_key",
        "attr_value",
        "status_code_share",
        "attr_key_stddev_samp_status_code",
        "status_code_share_avg_3",
        "cumulative_status_code_percent_rank",
        F.lead("status_code_share_avg_3", 2, 0).over(w7).alias("next_status_code_share_avg_3"),
        F.avg("status_code_share").over(w_recent).alias("status_code_share_avg_2"),
    )
)

ranked = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w9))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .groupBy("level")
    .agg(
        F.round(F.sum("next_status_code_share_avg_3") / F.nullif(F.count(F.when(F.col("level") == "INFO", 1)), F.lit(0)), 2).alias("next_status_code_share_avg_3_per_info"),
        F.percentile_approx("status_code_share_avg_3", 0.5).alias("status_code_share_avg_3_p50"),
        F.round(F.avg("status_code_share_avg_2"), 2).alias("avg_status_code_share_avg_2"),
        F.max(F.when(F.col("attr_key") == "region", F.col("status_code_share_avg_2"))).alias("region_max_status_code_share_avg_2"),
        F.round(F.avg("status_code_share"), 2).alias("avg_status_code_share"),
    )
    .orderBy(F.col("status_code_share_avg_3_p50").asc_nulls_last())
)

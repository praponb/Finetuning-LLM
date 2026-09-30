from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("status_code", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("k").orderBy(F.col("is_flagged").asc_nulls_last())
w_recent = Window.partitionBy("k")
w4 = Window.orderBy("is_flagged", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("k").orderBy(F.col("is_flagged_dense_rank").asc_nulls_last())
w6 = Window.partitionBy("k").orderBy(F.col("running_status_code").desc())

lines = (
    df_app_logs
    .select("id", "log_ts", "status_code", F.explode("context").alias("k", "v"))
)

flattened_attrs = (
    lines
    .select(
        "id",
        "status_code",
        "k",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

enriched = (
    flattened_attrs
    .filter(~F.col("k").isin(["region", "version"]))
    .select(
        "*",
        F.last("status_code_high", ignorenulls=True).over(w).alias("last_known_status_code_high"),
        F.dense_rank().over(w2).alias("is_flagged_dense_rank"),
    )
)

with_windows = (
    enriched
    .withColumn("k_stddev_samp_last_known_status_code_high", F.stddev_samp("last_known_status_code_high").over(w_recent))
    .withColumn("running_status_code", F.sum("status_code").over(w4))
    .withColumn("is_flagged_dense_rank_rank", F.rank().over(w5))
)

latest = (
    with_windows
    .withColumn("dedup_rank", F.rank().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .groupBy(
        "k",
        F.when(F.col("is_flagged_dense_rank") >= 8, "PREMIUM").otherwise("STANDARD").alias("is_flagged_dense_rank_tier"),
    )
    .agg(
        F.count(F.when(F.col("k") == "region", 1)).alias("region_count"),
        F.round(F.avg("last_known_status_code_high"), 2).alias("avg_last_known_status_code_high"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("avg_last_known_status_code_high").asc_nulls_last())
)

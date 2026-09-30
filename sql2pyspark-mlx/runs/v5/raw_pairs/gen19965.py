from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy("discount_pct", "id")
w2 = Window.orderBy(F.col("discount_pct").desc(), "id")
w3 = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("channel").orderBy(F.col("cumulative_discount_pct").desc_nulls_first())
w_recent = Window.partitionBy("channel").orderBy(F.col("discount_pct_bucket").desc(), "id")
w_grp = Window.orderBy("discount_pct_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("channel", "promo_code").orderBy("discount_pct_bucket_2", "id")
w9 = Window.partitionBy("channel").orderBy("discount_pct_bucket", "id")
w10 = Window.partitionBy("channel").orderBy(F.col("cumulative_discount_pct").desc(), "id")

filtered = (
    df_promotions
    .filter((F.col("ends_on") >= F.to_date(F.lit("2023-12-07"))) & (F.col("channel").eqNullSafe(F.lit("mobile"))))
    .select(
        "*",
        F.concat_ws(",", "promo_code", "channel").alias("promo_code_channel_key"),
    )
)

with_windows = (
    filtered
    .filter(F.col("channel").isin(["mobile", "web"]))
    .select(
        "id",
        "channel",
        "promo_code",
        F.ntile(10).over(w).alias("discount_pct_bucket"),
        F.ntile(5).over(w2).alias("discount_pct_bucket_2"),
        F.avg("discount_pct").over(w3).alias("discount_pct_avg_5_prior"),
        F.sum("discount_pct").over(w4).alias("cumulative_discount_pct"),
    )
)

with_windows_promotions = (
    with_windows
    .select(
        "id",
        "channel",
        "discount_pct_bucket",
        "discount_pct_bucket_2",
        "cumulative_discount_pct",
        F.rank().over(w5).alias("cumulative_discount_pct_rank"),
        F.ntile(5).over(w_recent).alias("discount_pct_bucket_bucket"),
        F.last("channel", ignorenulls=True).over(w_grp).alias("last_known_channel"),
        F.row_number().over(w8).alias("seq_no"),
    )
)

filtered_promotions = (
    with_windows_promotions
    .filter(F.col("channel").isNotNull() & (F.col("discount_pct_bucket") == 7))
    .withColumn("discount_pct_bucket_num", F.col("discount_pct_bucket").cast("double"))
)

metrics = (
    filtered_promotions
    .withColumn("prev_discount_pct_bucket_2", F.lag("discount_pct_bucket_2").over(w9))
    .withColumn("prev_cumulative_discount_pct", F.lag("cumulative_discount_pct").over(w9))
    .withColumn("seq_no_2", F.row_number().over(w9))
)

latest = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w10))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "discount_pct_bucket",
        "discount_pct_bucket_2",
        "cumulative_discount_pct",
        "last_known_channel",
        "discount_pct_bucket_num",
        "prev_discount_pct_bucket_2",
        "prev_cumulative_discount_pct",
        "seq_no_2",
    )
)

result = (
    latest
    .groupBy(
        F.when(F.col("discount_pct_bucket_2") >= 7, "HIGH").otherwise("LOW").alias("discount_pct_bucket_2_tier"),
        F.when(F.col("prev_cumulative_discount_pct") >= 2, "PREMIUM").otherwise("STANDARD").alias("prev_cumulative_discount_pct_tier"),
    )
    .agg(
        F.percentile_approx("discount_pct_bucket_num", 0.95).alias("discount_pct_bucket_num_p95"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("discount_pct_bucket_2"), 2).alias("avg_discount_pct_bucket_2"),
    )
    .orderBy(F.col("distinct_ids").desc_nulls_last())
)

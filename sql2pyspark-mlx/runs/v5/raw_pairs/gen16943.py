from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promo_code").orderBy("discount_pct", "id")
w2 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc())
w3 = Window.partitionBy("channel", "promo_code").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("channel")
w6 = Window.partitionBy("promo_code").orderBy(F.col("running_max_discount_pct_cume_dist").desc_nulls_last(), "id")
w7 = Window.partitionBy("promo_code").orderBy("channel_stddev_samp_discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("promo_code").orderBy("running_max_discount_pct_cume_dist", "id")
w9 = Window.partitionBy("promo_code").orderBy("channel_stddev_samp_discount_pct", "id")
w10 = Window.partitionBy("promo_code").orderBy(F.col("running_max_discount_pct_cume_dist").desc(), "id")
w_recent = Window.orderBy("running_max_discount_pct_cume_dist_bucket", "id")
w12 = Window.orderBy("seq_no", "id")

deduped = (
    df_promotions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

scored = (
    deduped
    .select(
        "*",
        F.cume_dist().over(w2).alias("discount_pct_cume_dist"),
        F.last("channel", ignorenulls=True).over(w3).alias("last_known_channel"),
    )
)

metrics = (
    scored
    .filter(F.col("channel").isin(["web", "partner", "store"]))
    .select(
        "id",
        "promo_code",
        "starts_on",
        F.max("discount_pct_cume_dist").over(w_running).alias("running_max_discount_pct_cume_dist"),
        F.min("discount_pct_cume_dist").over(w_running).alias("running_min_discount_pct_cume_dist"),
        F.sum("discount_pct_cume_dist").over(w_running).alias("running_sum_discount_pct_cume_dist"),
        F.avg("discount_pct_cume_dist").over(w_running).alias("running_avg_discount_pct_cume_dist"),
        F.stddev_samp("discount_pct").over(w5).alias("channel_stddev_samp_discount_pct"),
    )
)

latest = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

with_windows = (
    latest
    .select(
        "id",
        "starts_on",
        "running_min_discount_pct_cume_dist",
        "running_sum_discount_pct_cume_dist",
        F.first("running_max_discount_pct_cume_dist", ignorenulls=True).over(w7).alias("first_known_running_max_discount_pct_cume_dist"),
        F.ntile(10).over(w8).alias("running_max_discount_pct_cume_dist_bucket"),
        F.ntile(3).over(w9).alias("channel_stddev_samp_discount_pct_bucket"),
        F.row_number().over(w10).alias("seq_no"),
    )
)

with_windows_promotions = (
    with_windows
    .select(
        "*",
        F.lag("channel_stddev_samp_discount_pct_bucket").over(w_recent).alias("prev_channel_stddev_samp_discount_pct_bucket"),
        F.lead("channel_stddev_samp_discount_pct_bucket").over(w_recent).alias("next_channel_stddev_samp_discount_pct_bucket"),
        F.lag("seq_no").over(w_recent).alias("prev_seq_no"),
        F.row_number().over(w_recent).alias("seq_no_2"),
        F.row_number().over(w12).alias("row_seq"),
    )
)

totals = (
    with_windows_promotions
    .groupBy(
        F.when(F.col("channel_stddev_samp_discount_pct_bucket") >= 7, "HIGH").otherwise("LOW").alias("channel_stddev_samp_discount_pct_bucket_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.max("next_channel_stddev_samp_discount_pct_bucket").alias("max_next_channel_stddev_samp_discount_pct_bucket"),
        F.avg("seq_no_2").alias("avg_seq_no_2"),
        F.max("prev_channel_stddev_samp_discount_pct_bucket").alias("max_prev_channel_stddev_samp_discount_pct_bucket"),
    )
)

result = totals.select("channel_stddev_samp_discount_pct_bucket_tier")

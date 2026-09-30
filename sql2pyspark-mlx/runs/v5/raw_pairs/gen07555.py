from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel", "promo_code")
w3 = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("discount_pct_share").desc())
w5 = Window.partitionBy("promo_code").orderBy(F.col("starts_on").desc())
w_running = Window.partitionBy("promo_code").orderBy("discount_pct_share_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_promotions
    .filter(F.col("ends_on").between("2024-04-01", "2025-09-04") & F.col("channel").isNotNull())
    .withColumn("starts_on_label", F.date_format("starts_on", "yyyy-MM-dd"))
)

with_windows = (
    valid
    .filter(F.month("ends_on").isin([2, 6, 8]))
    .select(
        "*",
        F.last("channel", ignorenulls=True).over(w).alias("last_known_channel"),
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w2)), 4).alias("discount_pct_share"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "promo_code",
        "starts_on",
        "discount_pct_share",
        F.first("channel", ignorenulls=True).over(w3).alias("first_known_channel"),
        F.cume_dist().over(w4).alias("discount_pct_share_cume_dist"),
    )
)

latest = (
    metrics
    .withColumn("dedup_rank", F.rank().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    latest
    .select(
        "*",
        F.avg("discount_pct_share_cume_dist").over(w_running).alias("running_avg_discount_pct_share_cume_dist"),
        F.sum("discount_pct_share_cume_dist").over(w_running).alias("running_sum_discount_pct_share_cume_dist"),
        F.max("discount_pct_share_cume_dist").over(w_running).alias("running_max_discount_pct_share_cume_dist"),
        F.count("discount_pct_share_cume_dist").over(w_running).alias("running_count_discount_pct_share_cume_dist"),
    )
)

result = (
    scored
    .groupBy("promo_code")
    .agg(
        F.count(F.when(F.col("promo_code") == "SPRING10", 1)).alias("spring10_count"),
        F.round(F.avg("discount_pct_share"), 2).alias("avg_discount_pct_share"),
        F.count(F.when(F.col("promo_code") == "BUNDLE", 1)).alias("bundle_rows"),
        F.max("running_sum_discount_pct_share_cume_dist").alias("max_running_sum_discount_pct_share_cume_dist"),
    )
    .filter(F.col("spring10_count") >= 1)
    .orderBy(F.col("promo_code").asc_nulls_last())
)

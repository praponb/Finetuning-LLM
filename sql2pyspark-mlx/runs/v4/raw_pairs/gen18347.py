from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("starts_on").desc(), "id")
w2 = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("channel").orderBy("discount_pct", "id")
w4 = Window.partitionBy("promo_code", "channel").orderBy("dedup_rank", "id").rowsBetween(-6, Window.currentRow)
w5 = Window.partitionBy("promo_code", "channel").orderBy(F.col("starts_on").desc(), "id")
w6 = Window.orderBy("discount_pct_avg_3_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("channel", "promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("channel").orderBy(F.col("discount_pct_avg_3_prior").desc(), "id")
w9 = Window.orderBy("discount_pct_avg_6", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("running_discount_pct_avg_3_prior", "id")
w11 = Window.partitionBy("promo_code").orderBy("running_discount_pct_avg_3_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("promotion_promo_code", "promo_code")
w13 = Window.orderBy("dedup_rank", "id", "promo_id").rowsBetween(-2, Window.currentRow)
w14 = Window.partitionBy("promotion_promo_code", "promo_code").orderBy("row_seq_2", "id", "promo_id")
w15 = Window.orderBy("promotion_discount_pct", "id", "promo_id")
w16 = Window.partitionBy("channel").orderBy("first_known_discount_pct_bucket", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w17 = Window.partitionBy("row_seq_tier").orderBy(F.col("prev_row_seq_2_p50").desc())

latest = (
    df_promotions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    latest
    .select(
        "*",
        F.avg("discount_pct").over(w2).alias("discount_pct_avg_3_prior"),
        F.ntile(10).over(w3).alias("discount_pct_bucket"),
    )
)

enriched = (
    scored
    .withColumn("discount_pct_avg_6", F.avg("discount_pct").over(w4))
    .withColumn("row_seq", F.row_number().over(w5))
    .withColumn("first_known_discount_pct_bucket", F.first("discount_pct_bucket", ignorenulls=True).over(w6))
    .withColumn("running_discount_pct_avg_3_prior", F.sum("discount_pct_avg_3_prior").over(w7))
    .withColumn("row_seq_2", F.row_number().over(w8))
)

with_windows = (
    enriched
    .select(
        "*",
        F.first("discount_pct_avg_6", ignorenulls=True).over(w9).alias("first_known_discount_pct_avg_6"),
        F.ntile(5).over(w10).alias("running_discount_pct_avg_3_prior_bucket"),
        F.sum("discount_pct_avg_3_prior").over(w11).alias("running_discount_pct_avg_3_prior_2"),
    )
)

attributed = (
    with_windows
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & F.col("e.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

with_windows_promotions = (
    attributed
    .withColumn("promotion_discount_pct_share", F.round((F.col("promotion_discount_pct") / F.sum("promotion_discount_pct").over(w12)), 4))
    .withColumn("discount_pct_bucket_avg_2", F.avg("discount_pct_bucket").over(w13))
)

scored_promotions = (
    with_windows_promotions
    .withColumn("event_seq", F.row_number().over(w14))
    .withColumn("prev_row_seq_2", F.lag("row_seq_2", 1).over(w15))
    .withColumn("running_discount_pct_avg_6", F.sum("discount_pct_avg_6").over(w16))
)

result = (
    scored_promotions
    .groupBy(
        F.when(F.col("row_seq") >= 7, "HIGH").otherwise("LOW").alias("row_seq_tier"),
        F.date_trunc("quarter", "starts_on").alias("quarter_start"),
    )
    .agg(
        F.sum(F.when(F.col("promotion_promo_code") == "BUNDLE", F.col("first_known_discount_pct_avg_6")).otherwise(0)).alias("bundle_first_known_discount_pct_avg_6"),
        F.percentile_approx("prev_row_seq_2", 0.5).alias("prev_row_seq_2_p50"),
    )
    .filter(F.col("bundle_first_known_discount_pct_avg_6") >= 0)
    .withColumn("prev_row_seq_2_p50_dense_rank", F.dense_rank().over(w17))
)

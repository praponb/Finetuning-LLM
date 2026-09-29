from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("promo_code").orderBy("promo_code_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("promo_code_length", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("promo_code")
w5 = Window.orderBy("promo_code_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("channel").orderBy("discount_pct_scaled_avg_2", "id")
w7 = Window.partitionBy("promo_code").orderBy(F.col("first_known_promo_code_length").desc(), "id")

valid = (
    df_promotions
    .filter((F.col("ends_on") >= F.lit("2024-02-12").cast("date")) & (F.col("ends_on") < "2025-07-23"))
    .select(
        "*",
        F.trim("channel").alias("clean_channel"),
        F.round((F.col("discount_pct") * 1.1), 2).alias("discount_pct_scaled"),
    )
)

valid_promotions = (
    valid
    .filter((F.col("ends_on") >= F.lit("2023-12-31").cast("date")) & F.month("starts_on").isin([8, 11, 12]))
    .withColumn("promo_code_length", F.length("promo_code"))
    .withColumn("ends_on_label", F.date_format("ends_on", "MMM yyyy"))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("OTHER")))
)

scored = (
    valid_promotions
    .select(
        "*",
        F.first("discount_pct_scaled", ignorenulls=True).over(w).alias("first_known_discount_pct_scaled"),
        F.sum("promo_code_length").over(w2).alias("running_promo_code_length"),
        F.avg("discount_pct_scaled").over(w3).alias("discount_pct_scaled_avg_2"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.stddev_samp("discount_pct_scaled").over(w4).alias("promo_code_stddev_samp_discount_pct_scaled"),
        F.first("promo_code_length", ignorenulls=True).over(w5).alias("first_known_promo_code_length"),
    )
)

enriched = (
    metrics
    .withColumn("discount_pct_scaled_avg_2_bucket", F.ntile(3).over(w6))
    .withColumn("seq_no", F.row_number().over(w7))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("discount_pct_scaled_avg_2_bucket") >= 7, "PREMIUM").otherwise("STANDARD").alias("discount_pct_scaled_avg_2_bucket_tier"),
    )
    .agg(
        F.count_if(F.col("promo_code") == "VIP").alias("vip_events"),
        F.sum(F.when(F.col("channel") == "partner", F.col("discount_pct_scaled_avg_2")).otherwise(0)).alias("partner_discount_pct_scaled_avg_2"),
        F.count(F.when(F.col("promo_code") == "BUNDLE", 1)).alias("bundle_rows"),
    )
    .orderBy(F.col("bundle_rows").desc_nulls_first())
)

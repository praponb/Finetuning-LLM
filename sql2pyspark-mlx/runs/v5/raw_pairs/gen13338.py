from pyspark.sql import functions as F

valid = (
    df_promotions
    .filter((F.col("starts_on") >= F.lit("2023-06-04").cast("date")) & (F.col("discount_pct") < 0.32))
    .select(
        "*",
        F.when(F.col("discount_pct") >= 0.36, "High").when(F.col("discount_pct") >= 0.18, "Medium").otherwise("Low").alias("discount_pct_tier"),
        F.round("discount_pct", 1).alias("discount_pct_rounded"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_filled"),
    )
)

result = (
    valid
    .groupBy("channel")
    .pivot("promo_code", ["WELCOME5", "SPRING10"])
    .agg(F.avg("discount_pct").alias("amount"), F.count(F.lit(1)).alias("item_count"))
)

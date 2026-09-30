from pyspark.sql import functions as F

valid = (
    df_promotions
    .filter(F.col("ends_on") > "2024-07-01")
    .select(
        "*",
        F.round((F.col("discount_pct") * 0.85), 2).alias("adjusted_discount_pct"),
        F.round("discount_pct", 1).alias("discount_pct_rounded"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_clean"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("discount_pct") >= 0.28, "HIGH").otherwise("LOW").alias("discount_pct_tier"),
    )
    .agg(
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_rows"),
        F.sum(F.when(F.col("channel") == "partner", F.col("discount_pct_rounded")).otherwise(0)).alias("partner_discount_pct_rounded"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("adjusted_discount_pct").alias("min_adjusted_discount_pct"),
    )
    .orderBy(F.col("partner_discount_pct_rounded").desc_nulls_first())
)

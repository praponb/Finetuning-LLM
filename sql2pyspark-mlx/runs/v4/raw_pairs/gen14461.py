from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select("id", "effective_from", "currency", F.explode("tier_prices").alias("val"))
)

result = (
    lines
    .groupBy(F.when(F.col("val") >= 77, "HIGH").otherwise("LOW").alias("val_tier"))
    .agg(
        F.sum(F.when(F.col("currency") == "THB", F.col("val"))).alias("thb_total_val"),
        F.round(F.avg("val"), 2).alias("avg_val"),
        F.countDistinct("currency").alias("distinct_currencies"),
    )
)

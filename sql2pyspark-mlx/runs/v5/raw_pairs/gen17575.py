from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "effective_from", "currency", F.explode("tier_limits").alias("val"))
)

result = (
    exploded
    .groupBy(
        F.when(F.col("val") >= 68, "PREMIUM").otherwise("STANDARD").alias("val_tier"),
        "currency",
    )
    .agg(
        F.count(F.when(F.col("currency") == "THB", 1)).alias("thb_rows"),
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_rows"),
        F.round(F.avg("val"), 2).alias("avg_val"),
    )
)

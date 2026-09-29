from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("val"),
    )
)

result = (
    exploded
    .groupBy("plan")
    .agg(
        F.count_if(F.col("plan") == "enterprise").alias("enterprise_events"),
        F.count(F.when(F.col("currency") == "THB", 1)).alias("thb_rows"),
        F.round(F.sum("val") / F.nullif(F.count(F.when(F.col("currency") == "THB", 1)), F.lit(0)), 2).alias("val_per_thb"),
        F.round(F.sum("val") / F.nullif(F.count(F.when(F.col("currency") == "USD", 1)), F.lit(0)), 2).alias("val_per_usd"),
    )
)

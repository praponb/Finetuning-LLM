from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select("id", "currency", F.explode("tier_limits").alias("tier_value"))
)

result = (
    lines
    .groupBy("currency")
    .agg(
        F.count(F.when(F.col("currency") == "THB", 1)).alias("thb_rows"),
        F.round(F.avg("tier_value"), 2).alias("avg_tier_value"),
    )
    .filter(F.col("avg_tier_value") > 0)
    .orderBy(F.col("currency").desc())
)

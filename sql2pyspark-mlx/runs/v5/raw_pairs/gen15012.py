from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("tier_value"),
    )
)

result = (
    exploded
    .cube("currency", F.date_trunc("WEEK", "effective_from").alias("week_start"))
    .agg(
        F.round(F.avg("tier_value"), 2).alias("avg_tier_value"),
        F.avg(F.when(F.col("currency") == "USD", F.col("tier_value"))).alias("usd_avg_tier_value"),
        F.count("*").alias("event_count"),
        F.sum(F.when(F.col("currency") == "USD", F.col("tier_value")).otherwise(0)).alias("usd_tier_value"),
    )
    .orderBy(F.col("usd_tier_value").desc())
)

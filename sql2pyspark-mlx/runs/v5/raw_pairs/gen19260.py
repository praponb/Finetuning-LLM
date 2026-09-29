from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_prices").alias("val"))
)

result = (
    exploded
    .withColumn("val_tier", F.when(F.col("val") >= 66, "PREMIUM").otherwise("STANDARD"))
    .cube("currency", "val_tier", "plan")
    .agg(
        F.count_if(F.col("currency") == "USD").alias("usd_events"),
        F.percentile_approx("val", 0.95).alias("val_p95"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_rows"),
        F.round(F.sum("val") / F.nullif(F.count(F.when(F.col("plan") == "starter", 1)), F.lit(0)), 2).alias("val_per_starter"),
        F.percentile_approx("val", 0.9).alias("val_p90"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("val_p90") >= 0)
    .orderBy(F.col("usd_events").desc())
)

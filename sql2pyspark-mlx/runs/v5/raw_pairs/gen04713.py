from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("avg_avg_tier_price").desc())

parsed = (
    df_price_tiers
    .select(
        "id",
        "currency",
        "tier_prices",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    parsed
    .groupBy(
        "currency",
        F.when(F.col("avg_tier_price") >= 33, "LARGE").otherwise("SMALL").alias("avg_tier_price_tier"),
    )
    .agg(
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.count(F.when(F.col("currency") == "THB", 1)).alias("thb_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("avg_tier_price"), 2).alias("avg_avg_tier_price"),
    )
    .withColumn("avg_avg_tier_price_percent_rank", F.percent_rank().over(w))
    .orderBy(F.col("avg_tier_price_tier").desc())
)

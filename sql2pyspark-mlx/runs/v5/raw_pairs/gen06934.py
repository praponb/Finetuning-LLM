from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("avg_tier_price_tier").orderBy(F.col("entry_price_per_usd").desc())

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("avg_tier_price") >= 26, "PREMIUM").otherwise("STANDARD").alias("avg_tier_price_tier"),
        "currency",
    )
    .agg(
        F.round(F.sum("entry_price") / F.nullif(F.count(F.when(F.col("currency") == "USD", 1)), F.lit(0)), 2).alias("entry_price_per_usd"),
        F.countDistinct("plan").alias("distinct_plans"),
        F.sum(F.when(F.col("currency") == "THB", F.col("avg_tier_price"))).alias("thb_total_avg_tier_price"),
    )
    .withColumn("entry_price_per_usd_percent_rank", F.percent_rank().over(w))
    .orderBy(F.col("currency").desc())
)

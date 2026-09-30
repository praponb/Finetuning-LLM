from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("starter_avg_tier_price").desc())

unpacked = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_limits",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    unpacked
    .groupBy("plan", F.date_trunc("QUARTER", "effective_from").alias("quarter_start"))
    .agg(
        F.countDistinct("plan").alias("distinct_plans"),
        F.count(F.when(F.col("plan") == "pro", 1)).alias("pro_count"),
        F.sum(F.when(F.col("plan") == "starter", F.col("avg_tier_price")).otherwise(0)).alias("starter_avg_tier_price"),
        F.round(F.avg("entry_price"), 2).alias("avg_entry_price"),
    )
    .withColumn("starter_avg_tier_price_percent_rank", F.percent_rank().over(w))
)

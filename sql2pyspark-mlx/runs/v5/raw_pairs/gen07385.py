from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("avg_avg_tier_price").desc())

parsed = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "discounts",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    parsed
    .groupBy(
        "currency",
        F.date_trunc("month", "effective_from").alias("activity_month"),
        F.when(F.col("entry_price") >= 30, "LARGE").otherwise("SMALL").alias("entry_price_tier"),
    )
    .agg(
        F.round(F.avg("avg_tier_price"), 2).alias("avg_avg_tier_price"),
        F.percentile_approx("entry_price", [0.25, 0.5, 0.75]).alias("entry_price_quartiles"),
        F.max(F.when(F.col("currency") == "EUR", F.col("avg_tier_price"))).alias("eur_max_avg_tier_price"),
        F.countDistinct("currency").alias("distinct_currencies"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
    )
    .filter(F.col("avg_avg_tier_price") > 0)
    .withColumn("avg_avg_tier_price_percent_rank", F.percent_rank().over(w))
)

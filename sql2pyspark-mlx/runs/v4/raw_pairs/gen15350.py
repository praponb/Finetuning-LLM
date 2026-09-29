from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.partitionBy("currency").orderBy("tier_value_filled")
w3 = Window.orderBy(F.col("tier_value").desc())
w4 = Window.partitionBy("currency").orderBy("currency_count_tier_value")
w_grp = Window.partitionBy("currency").orderBy("tier_value_rounded")

exploded = (
    df_price_tiers
    .select("id", "currency", F.explode("tier_limits").alias("tier_value"))
)

cleaned = (
    exploded
    .filter(F.col("tier_value") >= 41)
    .withColumn("tier_value_rounded", F.round("tier_value", 0))
    .withColumn("tier_value_filled", F.coalesce("tier_value", F.lit(0)))
)

scored = (
    cleaned
    .withColumn("currency_count_tier_value", F.count("tier_value").over(w))
    .withColumn("tier_value_filled_rank", F.rank().over(w2))
    .withColumn("tier_value_cume_dist", F.cume_dist().over(w3))
)

with_windows = (
    scored
    .withColumn("currency_count_tier_value_cume_dist", F.cume_dist().over(w4))
    .withColumn("currency_avg_tier_value_cume_dist", F.avg("tier_value_cume_dist").over(w))
    .withColumn("tier_value_rounded_percent_rank", F.percent_rank().over(w_grp))
)

result = (
    with_windows
    .withColumn("currency_avg_tier_value_cume_dist_tier", F.when(F.col("currency_avg_tier_value_cume_dist") >= 1, "HIGH").otherwise("LOW"))
    .cube("currency_avg_tier_value_cume_dist_tier")
    .agg(
        F.percentile_approx("tier_value_rounded_percent_rank", [0.25, 0.5, 0.75]).alias("tier_value_rounded_percent_rank_quartiles"),
        F.round(F.sum("currency_count_tier_value") / F.nullif(F.count(F.when(F.col("currency") == "EUR", 1)), F.lit(0)), 2).alias("currency_count_tier_value_per_eur"),
        F.max("tier_value_filled_rank").alias("max_tier_value_filled_rank"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.avg("currency_count_tier_value").alias("avg_currency_count_tier_value"),
        F.percentile_approx("currency_count_tier_value_cume_dist", 0.95).alias("currency_count_tier_value_cume_dist_p95"),
        F.grouping("currency_avg_tier_value_cume_dist_tier").alias("grouping_id"),
    )
    .filter(F.col("max_tier_value_filled_rank") >= 0)
)

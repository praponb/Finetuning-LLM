from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("tier_value").desc())
w2 = Window.partitionBy("currency")
w3 = Window.partitionBy("plan")
w4 = Window.partitionBy("currency", "plan")

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        F.explode_outer("tier_limits").alias("tier_value"),
    )
)

metrics = (
    exploded
    .withColumn("tier_value_percent_rank", F.percent_rank().over(w))
    .withColumn("currency_avg_tier_value", F.avg("tier_value").over(w2))
)

with_windows = (
    metrics
    .select(
        "*",
        F.min("currency_avg_tier_value").over(w2).alias("currency_min_currency_avg_tier_value"),
        F.count("tier_value").over(w3).alias("plan_count_tier_value"),
    )
)

with_windows_price_tiers = (
    with_windows
    .select(
        "*",
        F.round((F.col("plan_count_tier_value") / F.sum("plan_count_tier_value").over(w3)), 4).alias("plan_count_tier_value_share"),
        F.sum("currency_min_currency_avg_tier_value").over(w2).alias("currency_sum_currency_min_currency_avg_tier_value"),
        F.avg("tier_value").over(w2).alias("currency_avg_tier_value_2"),
        F.avg("currency_min_currency_avg_tier_value").over(w4).alias("currency_avg_currency_min_currency_avg_tier_value"),
    )
)

result = (
    with_windows_price_tiers
    .select(
        "currency",
        "currency_avg_tier_value",
        "currency_min_currency_avg_tier_value",
        "plan_count_tier_value",
        "plan_count_tier_value_share",
        "currency_sum_currency_min_currency_avg_tier_value",
        "currency_avg_currency_min_currency_avg_tier_value",
        F.when(F.col("currency").rlike("^(THB|EUR)$"), "EXTENDED").when(F.col("currency").rlike("^(USD)$"), "PRIORITY").otherwise("CORE").alias("currency_zone"),
    )
    .orderBy(F.col("plan_count_tier_value_share").asc_nulls_last(), "currency")
)

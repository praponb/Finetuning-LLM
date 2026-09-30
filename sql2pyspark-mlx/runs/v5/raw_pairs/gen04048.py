from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "currency").orderBy("tier_value")
w2 = Window.partitionBy("currency")
w3 = Window.partitionBy("plan", "currency").orderBy(F.col("tier_value").desc_nulls_last())
w4 = Window.partitionBy("plan", "currency")
w5 = Window.partitionBy("plan").orderBy(F.col("currency_avg_tier_value").desc())
w6 = Window.partitionBy("plan").orderBy(F.col("tier_value").desc())
w7 = Window.partitionBy("plan")
w8 = Window.partitionBy("plan").orderBy(F.col("plan_sum_currency_avg_tier_value").desc())
w9 = Window.partitionBy("plan").orderBy(F.col("plan_avg_tier_value").desc_nulls_last())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        F.explode_outer("tier_prices").alias("tier_value"),
    )
)

metrics = (
    lines
    .filter(F.col("currency").isin(["EUR", "USD"]))
    .select(
        "plan",
        "tier_value",
        F.dense_rank().over(w).alias("tier_value_dense_rank"),
        F.avg("tier_value").over(w2).alias("currency_avg_tier_value"),
        F.percent_rank().over(w3).alias("tier_value_percent_rank"),
        F.avg("tier_value").over(w4).alias("plan_avg_tier_value"),
    )
)

scored = (
    metrics
    .withColumn("currency_avg_tier_value_rank", F.rank().over(w5))
    .withColumn("tier_value_cume_dist", F.cume_dist().over(w6))
    .withColumn("plan_sum_currency_avg_tier_value", F.sum("currency_avg_tier_value").over(w7))
    .withColumn("tier_value_dense_rank_2", F.dense_rank().over(w6))
)

enriched = (
    scored
    .select(
        "plan",
        "tier_value",
        "tier_value_dense_rank",
        "currency_avg_tier_value",
        "tier_value_percent_rank",
        "plan_avg_tier_value",
        "currency_avg_tier_value_rank",
        "tier_value_cume_dist",
        "plan_sum_currency_avg_tier_value",
        F.percent_rank().over(w8).alias("plan_sum_currency_avg_tier_value_percent_rank"),
        F.cume_dist().over(w9).alias("plan_avg_tier_value_cume_dist"),
    )
)

result = (
    enriched
    .select(
        "plan",
        "tier_value",
        "tier_value_percent_rank",
        "tier_value_cume_dist",
        "plan_sum_currency_avg_tier_value",
        "plan_avg_tier_value_cume_dist",
        F.stddev_samp("tier_value_cume_dist").over(w7).alias("plan_stddev_samp_tier_value_cume_dist"),
    )
)

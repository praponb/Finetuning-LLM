from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("currency")
w2 = Window.partitionBy("currency").orderBy(F.col("tier_value").asc_nulls_first())
w3 = Window.orderBy(F.col("currency_sum_tier_value_2").asc_nulls_first())
w4 = Window.orderBy(F.col("tier_value_percent_rank").asc_nulls_last())

lines = (
    df_price_tiers
    .select("id", "currency", F.explode_outer("discounts").alias("tier_value"))
)

base = (
    lines
    .filter(F.col("currency").isin(["EUR", "THB", "USD"]))
    .withColumn("currency_lower", F.lower("currency"))
    .withColumn("currency_group", F.when(F.col("currency").isin(["USD", "THB"]), "Core").otherwise("Other"))
)

metrics = (
    base
    .select(
        "id",
        "currency_lower",
        "currency_group",
        F.sum("tier_value").over(w_part).alias("currency_sum_tier_value"),
        F.sum("tier_value").over(w_part).alias("currency_sum_tier_value_2"),
        F.percent_rank().over(w2).alias("tier_value_percent_rank"),
    )
)

cleaned = (
    metrics
    .filter(F.col("tier_value_percent_rank").between(0.27, 0.59))
    .withColumn("currency_sum_tier_value_minus_currency_sum_tier_value_2", (F.col("currency_sum_tier_value") - F.col("currency_sum_tier_value_2")))
)

metrics_price_tiers = (
    cleaned
    .withColumn("currency_sum_tier_value_2_dense_rank", F.dense_rank().over(w3))
    .withColumn("tier_value_percent_rank_percent_rank", F.percent_rank().over(w4))
)

result = (
    metrics_price_tiers
    .select(
        "id",
        "currency_lower",
        "currency_sum_tier_value",
        "currency_sum_tier_value_2",
        "tier_value_percent_rank",
        "currency_sum_tier_value_2_dense_rank",
        "tier_value_percent_rank_percent_rank",
    )
)

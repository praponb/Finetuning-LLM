from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency", "plan")
w2 = Window.partitionBy("currency")
w3 = Window.partitionBy("plan", "currency").orderBy(F.col("currency_avg_tier_value").desc())
w4 = Window.partitionBy("plan").orderBy(F.col("currency_avg_tier_value_2").asc_nulls_last())
w5 = Window.partitionBy("currency", "plan").orderBy(F.col("currency_avg_tier_value").asc_nulls_last())
w6 = Window.partitionBy("plan")
w_part = Window.orderBy(F.col("currency_avg_tier_value").desc_nulls_first())
w8 = Window.partitionBy("plan").orderBy(F.col("currency_avg_tier_value").desc_nulls_first())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        F.explode_outer("tier_prices").alias("tier_value"),
    )
)

scored = (
    exploded
    .select(
        "*",
        F.avg("tier_value").over(w).alias("currency_avg_tier_value"),
        F.avg("tier_value").over(w2).alias("currency_avg_tier_value_2"),
    )
)

metrics = (
    scored
    .withColumn("currency_avg_tier_value_rank", F.rank().over(w3))
    .withColumn("currency_avg_tier_value_2_rank", F.rank().over(w4))
    .withColumn("currency_max_tier_value", F.max("tier_value").over(w2))
    .withColumn("currency_avg_tier_value_dense_rank", F.dense_rank().over(w5))
    .withColumn("plan_stddev_samp_currency_avg_tier_value", F.stddev_samp("currency_avg_tier_value").over(w6))
)

metrics_price_tiers = (
    metrics
    .select(
        "*",
        F.rank().over(w_part).alias("currency_avg_tier_value_rank_2"),
        F.percent_rank().over(w8).alias("currency_avg_tier_value_percent_rank"),
    )
)

result = (
    metrics_price_tiers
    .groupBy("currency")
    .pivot("plan", ["basic", "pro"])
    .agg(
        F.max("currency_avg_tier_value_2").alias("value_sum"),
        F.count(F.lit(1)).alias("item_count"),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").desc_nulls_last())
w2 = Window.orderBy(F.col("tier_value").asc_nulls_first())
w3 = Window.partitionBy("currency", "plan")
w4 = Window.partitionBy("currency")
w5 = Window.partitionBy("plan", "currency")
w6 = Window.partitionBy("plan").orderBy(F.col("plan_max_tier_value").asc_nulls_first())
w7 = Window.partitionBy("plan").orderBy(F.col("currency_sum_tier_value_adjusted").desc_nulls_last())
w8 = Window.partitionBy("plan")
w9 = Window.orderBy(F.col("tier_value_dense_rank").desc_nulls_last())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        F.explode_outer("tier_prices").alias("tier_value"),
    )
)

filtered = (
    lines
    .filter(F.col("currency") != "THB")
    .select(
        "*",
        F.when(F.col("currency").isin(["THB", "EUR"]), "Core").otherwise("Other").alias("currency_group"),
        F.substring("plan", 1, 2).alias("plan_prefix"),
    )
)

scored = (
    filtered
    .select(
        "id",
        "plan",
        F.dense_rank().over(w).alias("tier_value_dense_rank"),
        F.percent_rank().over(w2).alias("tier_value_percent_rank"),
        F.sum("tier_value").over(w3).alias("currency_sum_tier_value"),
        F.stddev_samp("tier_value").over(w4).alias("currency_stddev_samp_tier_value"),
        F.max("tier_value").over(w5).alias("plan_max_tier_value"),
    )
)

base = (
    scored
    .filter(F.col("plan") == "enterprise")
    .select(
        "*",
        F.round((F.col("currency_sum_tier_value") * 1.15), 2).alias("currency_sum_tier_value_adjusted"),
    )
)

enriched = (
    base
    .withColumn("plan_max_tier_value_percent_rank", F.percent_rank().over(w6))
    .withColumn("currency_sum_tier_value_adjusted_percent_rank", F.percent_rank().over(w7))
    .withColumn("plan_count_tier_value_percent_rank", F.count("tier_value_percent_rank").over(w8))
    .withColumn("tier_value_dense_rank_dense_rank", F.dense_rank().over(w9))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("tier_value_dense_rank_dense_rank") >= 8, "HIGH").otherwise("LOW").alias("tier_value_dense_rank_dense_rank_tier"),
        "plan",
    )
    .agg(
        F.avg(F.when(F.col("plan") == "starter", F.col("currency_sum_tier_value_adjusted_percent_rank"))).alias("starter_avg_currency_sum_tier_value_adjusted_percent_rank"),
        F.round(F.sum("plan_max_tier_value") / F.nullif(F.count(F.when(F.col("plan") == "basic", 1)), F.lit(0)), 2).alias("plan_max_tier_value_per_basic"),
        F.sum(F.when(F.col("plan") == "pro", F.col("tier_value_percent_rank")).otherwise(0)).alias("pro_tier_value_percent_rank"),
    )
)

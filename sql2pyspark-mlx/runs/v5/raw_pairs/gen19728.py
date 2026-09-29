from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").desc())
w2 = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_first())
w3 = Window.partitionBy("plan", "currency").orderBy(F.col("tier_value").desc_nulls_first())
w4 = Window.orderBy(F.col("tier_value").desc_nulls_last())
w5 = Window.partitionBy("currency")
w6 = Window.partitionBy("plan", "currency")
w7 = Window.partitionBy("plan")

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("tier_value"),
    )
)

base = (
    lines
    .filter(F.col("currency").isin(["THB", "USD"]))
    .select(
        "*",
        F.trim("plan").alias("clean_plan"),
        F.upper("currency").alias("currency_upper"),
    )
)

scored = (
    base
    .filter(F.col("plan") != "starter")
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "currency_upper",
        F.cume_dist().over(w).alias("tier_value_cume_dist"),
        F.cume_dist().over(w2).alias("tier_value_cume_dist_2"),
        F.dense_rank().over(w3).alias("tier_value_dense_rank"),
        F.cume_dist().over(w4).alias("tier_value_cume_dist_3"),
        F.sum("tier_value").over(w5).alias("currency_sum_tier_value"),
    )
)

metrics = (
    scored
    .withColumn("plan_stddev_samp_currency_sum_tier_value", F.stddev_samp("currency_sum_tier_value").over(w6))
    .withColumn("plan_count_tier_value_cume_dist_3", F.count("tier_value_cume_dist_3").over(w7))
    .withColumn("currency_max_tier_value_cume_dist_3", F.max("tier_value_cume_dist_3").over(w5))
)

result = (
    metrics
    .select(
        "currency",
        "currency_upper",
        "tier_value_cume_dist",
        "tier_value_cume_dist_2",
        "tier_value_dense_rank",
        "tier_value_cume_dist_3",
        "plan_stddev_samp_currency_sum_tier_value",
        "currency_max_tier_value_cume_dist_3",
        F.when(F.col("currency").rlike("^(THB|EUR)$"), "CORE").otherwise("GROUP_B").alias("currency_zone"),
    )
    .orderBy(
        F.col("currency").desc_nulls_last(),
        F.col("currency_max_tier_value_cume_dist_3").desc(),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").asc_nulls_first())
w2 = Window.orderBy(F.col("tier_value").desc())
w3 = Window.partitionBy("plan")
w4 = Window.partitionBy("plan").orderBy(F.col("tier_value").asc_nulls_last())
w5 = Window.partitionBy("currency")
w6 = Window.partitionBy("currency", "plan")
w_hist = Window.partitionBy("plan").orderBy(F.col("tier_value_percent_rank_2").desc_nulls_last())
w8 = Window.orderBy(F.col("currency_count_tier_value").asc_nulls_first())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        F.explode_outer("tier_limits").alias("tier_value"),
    )
)

base = (
    exploded
    .filter(F.col("tier_value") < 62)
    .withColumn("currency_group", F.when(F.col("currency").isin(["EUR", "USD"]), "Tier 1").otherwise("Other"))
)

valid = (
    base
    .filter(F.col("tier_value") >= 28)
    .withColumn("plan_title", F.initcap(F.trim("plan")))
)

with_windows = (
    valid
    .select(
        "*",
        F.dense_rank().over(w).alias("tier_value_dense_rank"),
        F.percent_rank().over(w2).alias("tier_value_percent_rank"),
        F.count("tier_value").over(w3).alias("plan_count_tier_value"),
        F.percent_rank().over(w4).alias("tier_value_percent_rank_2"),
        F.count("tier_value").over(w5).alias("currency_count_tier_value"),
    )
)

base_price_tiers = (
    with_windows
    .filter(F.col("currency_count_tier_value") >= 8)
    .select(
        "*",
        F.when(F.col("tier_value") >= 80, "High").when(F.col("tier_value") >= 22, "Medium").otherwise("Low").alias("tier_value_band"),
    )
)

result = (
    base_price_tiers
    .filter(F.col("tier_value_percent_rank") > 0.2)
    .select(
        "*",
        F.max("tier_value").over(w5).alias("currency_max_tier_value"),
        F.sum("tier_value_dense_rank").over(w6).alias("currency_sum_tier_value_dense_rank"),
        F.rank().over(w_hist).alias("tier_value_percent_rank_2_rank"),
        F.percent_rank().over(w8).alias("currency_count_tier_value_percent_rank"),
    )
)

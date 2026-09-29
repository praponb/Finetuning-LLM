from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("plan", "currency").orderBy("tier_value")
w_hist = Window.partitionBy("plan").orderBy(F.col("tier_value").asc_nulls_first())
w_part = Window.orderBy("tier_value_dense_rank_rounded")
w5 = Window.partitionBy("plan", "currency")
w_part_2 = Window.partitionBy("currency").orderBy(F.col("tier_value_dense_rank_rounded").desc())
w7 = Window.partitionBy("plan").orderBy(F.col("plan_sum_tier_value").asc_nulls_first())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("tier_value"),
    )
)

with_windows = (
    flattened
    .withColumn("plan_sum_tier_value", F.sum("tier_value").over(w))
    .withColumn("tier_value_dense_rank", F.dense_rank().over(w2))
    .withColumn("tier_value_rank", F.rank().over(w_hist))
)

base = (
    with_windows
    .filter((F.col("effective_from") > F.lit("2024-07-29").cast("date")) & ~F.col("currency").isin(["USD", "EUR"]))
    .select("*", F.dayofmonth("effective_from").alias("effective_from_day"))
)

valid = (
    base
    .filter(F.col("effective_from") >= F.lit("2024-06-30").cast("date"))
    .withColumn("tier_value_dense_rank_rounded", F.round("tier_value_dense_rank", 0))
    .withColumn("tier_value_dense_rank_whole", F.ceil("tier_value_dense_rank"))
)

enriched = (
    valid
    .withColumn("tier_value_dense_rank_rounded_percent_rank", F.percent_rank().over(w_part))
    .withColumn("plan_sum_plan_sum_tier_value", F.sum("plan_sum_tier_value").over(w5))
    .withColumn("tier_value_dense_rank_rounded_percent_rank_2", F.percent_rank().over(w_part_2))
    .withColumn("tier_value_dense_rank_share", F.round((F.col("tier_value_dense_rank") / F.sum("tier_value_dense_rank").over(w)), 4))
    .withColumn("plan_sum_tier_value_dense_rank", F.dense_rank().over(w7))
)

base_price_tiers = (
    enriched
    .filter(F.col("tier_value_dense_rank_rounded") >= 49)
    .withColumn("tier_value_int", F.col("tier_value").cast("int"))
)

result = (
    base_price_tiers
    .rollup(
        F.when(F.col("tier_value_dense_rank") >= 9, "HIGH").otherwise("LOW").alias("tier_value_dense_rank_tier"),
    )
    .agg(
        F.count(F.when(F.col("plan") == "pro", 1)).alias("pro_count"),
        F.round(F.sum("plan_sum_tier_value") / F.nullif(F.count(F.when(F.col("currency") == "USD", 1)), F.lit(0)), 2).alias("plan_sum_tier_value_per_usd"),
        F.round(F.sum("tier_value_int") / F.nullif(F.count(F.when(F.col("plan") == "starter", 1)), F.lit(0)), 2).alias("tier_value_int_per_starter"),
    )
    .orderBy("pro_count")
)

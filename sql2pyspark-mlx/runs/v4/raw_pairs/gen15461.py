from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy("element")
w_recent = Window.partitionBy("plan").orderBy(F.col("plan_sum_element").desc_nulls_last())

flattened = df_price_tiers.select("id", "plan", F.explode_outer("tier_prices").alias("element"))

with_windows = (
    flattened
    .select(
        "*",
        F.sum("element").over(w).alias("plan_sum_element"),
        F.rank().over(w2).alias("element_rank"),
    )
)

metrics = (
    with_windows
    .withColumn("plan_count_element_rank", F.count("element_rank").over(w))
    .withColumn("element_rank_share", F.round((F.col("element_rank") / F.sum("element_rank").over(w)), 4))
)

scored = (
    metrics
    .withColumn("plan_count_element_rank_2", F.count("element_rank").over(w))
    .withColumn("plan_min_element_rank_share", F.min("element_rank_share").over(w))
)

metrics_price_tiers = (
    scored
    .withColumn("element_rank_share_2", F.round((F.col("element_rank") / F.sum("element_rank").over(w)), 4))
    .withColumn("plan_sum_element_rank", F.rank().over(w_recent))
)

result = (
    metrics_price_tiers
    .select(
        "plan",
        "element_rank",
        "plan_count_element_rank",
        "element_rank_share",
        "plan_count_element_rank_2",
        "plan_min_element_rank_share",
        "element_rank_share_2",
        "plan_sum_element_rank",
    )
    .orderBy(F.col("plan_sum_element_rank").asc_nulls_last())
)

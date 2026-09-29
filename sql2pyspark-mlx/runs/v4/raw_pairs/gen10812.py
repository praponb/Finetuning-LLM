from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("plan")
w_ordered = Window.partitionBy("plan").orderBy(F.col("tier_value").desc_nulls_last())
w3 = Window.orderBy(F.col("tier_value_cume_dist").asc_nulls_last())
w4 = Window.partitionBy("plan").orderBy(F.col("tier_value_cume_dist").desc_nulls_first())

flattened = (
    df_price_tiers
    .select("id", "plan", F.explode_outer("discounts").alias("tier_value"))
)

scored = (
    flattened
    .select(
        "*",
        F.count("tier_value").over(w_hist).alias("plan_count_tier_value"),
        F.percent_rank().over(w_ordered).alias("tier_value_percent_rank"),
        F.cume_dist().over(w_ordered).alias("tier_value_cume_dist"),
    )
)

cleaned = (
    scored
    .filter(F.col("tier_value_percent_rank") <= 0.42)
    .select(
        "*",
        F.round("tier_value_cume_dist", 2).alias("tier_value_cume_dist_rounded"),
    )
)

enriched = (
    cleaned
    .withColumn("plan_max_tier_value_cume_dist_rounded", F.max("tier_value_cume_dist_rounded").over(w_hist))
    .withColumn("tier_value_cume_dist_cume_dist", F.cume_dist().over(w3))
    .withColumn("tier_value_cume_dist_dense_rank", F.dense_rank().over(w4))
    .withColumn("plan_max_plan_count_tier_value", F.max("plan_count_tier_value").over(w_hist))
)

result = (
    enriched
    .select(
        "id",
        "tier_value_cume_dist",
        "tier_value_cume_dist_rounded",
        "plan_max_tier_value_cume_dist_rounded",
        "tier_value_cume_dist_cume_dist",
    )
    .orderBy(
        F.col("tier_value_cume_dist").desc_nulls_first(),
        F.col("tier_value_cume_dist_rounded").desc(),
    )
)

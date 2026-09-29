from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("plan")
w_hist = Window.partitionBy("plan").orderBy(F.col("tier_value").asc_nulls_first())
w3 = Window.partitionBy("plan").orderBy(F.col("tier_value").desc_nulls_first())
w4 = Window.partitionBy("plan").orderBy(F.col("tier_value").asc_nulls_last())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("discounts").alias("tier_value"),
    )
)

result = (
    flattened
    .select(
        "id",
        "effective_from",
        F.stddev_samp("tier_value").over(w_recent).alias("plan_stddev_samp_tier_value"),
        F.max("tier_value").over(w_recent).alias("plan_max_tier_value"),
        F.cume_dist().over(w_hist).alias("tier_value_cume_dist"),
        F.percent_rank().over(w3).alias("tier_value_percent_rank"),
        F.percent_rank().over(w4).alias("tier_value_percent_rank_2"),
    )
)

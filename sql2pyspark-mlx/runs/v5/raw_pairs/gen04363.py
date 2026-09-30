from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_first())
w2 = Window.partitionBy("plan", "currency")
w3 = Window.orderBy(F.col("tier_value").asc_nulls_first())
w4 = Window.partitionBy("currency").orderBy(F.col("tier_value").asc_nulls_last())

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

result = (
    flattened
    .filter(F.col("currency").isin(["USD", "THB"]))
    .select(
        "id",
        "effective_from",
        "tier_value",
        F.cume_dist().over(w).alias("tier_value_cume_dist"),
        F.stddev_samp("tier_value").over(w2).alias("plan_stddev_samp_tier_value"),
        F.dense_rank().over(w3).alias("tier_value_dense_rank"),
        F.percent_rank().over(w4).alias("tier_value_percent_rank"),
    )
)

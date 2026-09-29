from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("plan", "currency").orderBy(F.col("tier_value").desc_nulls_last())
w2 = Window.orderBy(F.col("tier_value").desc())
w_running = Window.partitionBy("currency")
w4 = Window.orderBy(F.col("tier_value").desc_nulls_first())

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_limits").alias("tier_value"))
)

result = (
    flattened
    .select(
        "plan",
        "currency",
        F.dense_rank().over(w_part).alias("tier_value_dense_rank"),
        F.percent_rank().over(w2).alias("tier_value_percent_rank"),
        F.stddev_samp("tier_value").over(w_running).alias("currency_stddev_samp_tier_value"),
        F.cume_dist().over(w4).alias("tier_value_cume_dist"),
        F.stddev_samp("tier_value").over(w_running).alias("currency_stddev_samp_tier_value_2"),
    )
)

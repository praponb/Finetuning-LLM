from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").desc_nulls_last())
w2 = Window.partitionBy("currency")
w3 = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_last())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("tier_value"),
    )
)

result = (
    flattened
    .select(
        "*",
        F.rank().over(w).alias("tier_value_rank"),
        F.stddev_samp("tier_value").over(w2).alias("currency_stddev_samp_tier_value"),
        F.dense_rank().over(w3).alias("tier_value_dense_rank"),
    )
)

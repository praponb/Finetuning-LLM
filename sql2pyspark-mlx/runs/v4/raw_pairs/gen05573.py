from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.partitionBy("currency").orderBy(F.col("tier_value").asc_nulls_first())

lines = (
    df_price_tiers
    .select("id", "currency", F.explode_outer("discounts").alias("tier_value"))
)

result = (
    lines
    .select(
        "*",
        F.min("tier_value").over(w).alias("currency_min_tier_value"),
        F.dense_rank().over(w2).alias("tier_value_dense_rank"),
    )
)

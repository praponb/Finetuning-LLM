from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.partitionBy("currency").orderBy(F.col("tier_value").desc())

flattened = (
    df_price_tiers
    .select("id", "currency", F.explode_outer("tier_prices").alias("tier_value"))
)

result = (
    flattened
    .select(
        "*",
        F.max("tier_value").over(w).alias("currency_max_tier_value"),
        F.dense_rank().over(w2).alias("tier_value_dense_rank"),
    )
)

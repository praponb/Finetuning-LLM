from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy(F.col("tier_value").desc_nulls_last())
w2 = Window.orderBy(F.col("tier_value").desc())
w3 = Window.orderBy("tier_value")

lines = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("tier_prices").alias("tier_value"))
)

result = (
    lines
    .select(
        "*",
        F.cume_dist().over(w_recent).alias("tier_value_cume_dist"),
        F.rank().over(w2).alias("tier_value_rank"),
        F.percent_rank().over(w3).alias("tier_value_percent_rank"),
    )
)

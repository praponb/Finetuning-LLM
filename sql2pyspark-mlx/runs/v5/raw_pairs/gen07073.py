from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("tier_value")
w2 = Window.orderBy(F.col("tier_value").desc_nulls_last())
w3 = Window.orderBy(F.col("tier_value").desc())

exploded = df_price_tiers.select("id", F.explode_outer("tier_prices").alias("tier_value"))

result = (
    exploded
    .filter(F.col("tier_value") > 23)
    .select(
        "id",
        "tier_value",
        F.cume_dist().over(w).alias("tier_value_cume_dist"),
        F.rank().over(w2).alias("tier_value_rank"),
        F.rank().over(w3).alias("tier_value_rank_2"),
    )
)

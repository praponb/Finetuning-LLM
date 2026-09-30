from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("element").desc())
w2 = Window.orderBy(F.col("element").asc_nulls_first())
w3 = Window.orderBy("element")
w4 = Window.orderBy(F.col("element").desc_nulls_first())

exploded = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("tier_limits").alias("element"))
)

result = (
    exploded
    .select(
        "*",
        F.rank().over(w).alias("element_rank"),
        F.cume_dist().over(w2).alias("element_cume_dist"),
        F.rank().over(w3).alias("element_rank_2"),
        F.cume_dist().over(w4).alias("element_cume_dist_2"),
    )
)

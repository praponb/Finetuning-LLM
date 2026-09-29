from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("element")
w2 = Window.orderBy(F.col("element").asc_nulls_first())

flattened = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("tier_prices").alias("element"))
)

result = (
    flattened
    .select(
        "id",
        "effective_from",
        F.cume_dist().over(w).alias("element_cume_dist"),
        F.rank().over(w2).alias("element_rank"),
    )
)

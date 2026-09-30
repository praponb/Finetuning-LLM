from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("element")
w2 = Window.orderBy(F.col("element").desc_nulls_last())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("element"),
    )
)

result = (
    flattened
    .select(
        "*",
        F.cume_dist().over(w).alias("element_cume_dist"),
        F.cume_dist().over(w2).alias("element_cume_dist_2"),
    )
)

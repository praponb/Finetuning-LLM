from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("val")
w2 = Window.orderBy(F.col("val").desc_nulls_first())
w3 = Window.orderBy(F.col("val").desc())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("val"),
    )
)

result = (
    flattened
    .select(
        "*",
        F.cume_dist().over(w).alias("val_cume_dist"),
        F.dense_rank().over(w2).alias("val_dense_rank"),
        F.dense_rank().over(w3).alias("val_dense_rank_2"),
    )
)

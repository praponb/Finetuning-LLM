from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("val").desc_nulls_first())
w2 = Window.orderBy("val")
w3 = Window.partitionBy("currency", "plan").orderBy(F.col("val").desc())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("val"),
    )
)

result = (
    flattened
    .select(
        "plan",
        "effective_from",
        "currency",
        F.rank().over(w).alias("val_rank"),
        F.cume_dist().over(w2).alias("val_cume_dist"),
        F.cume_dist().over(w3).alias("val_cume_dist_2"),
    )
)

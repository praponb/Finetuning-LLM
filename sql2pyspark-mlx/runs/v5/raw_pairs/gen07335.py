from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("val").desc())
w_grp = Window.orderBy(F.col("val").asc_nulls_last())

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
        "currency",
        "val",
        F.rank().over(w).alias("val_rank"),
        F.dense_rank().over(w_grp).alias("val_dense_rank"),
        F.cume_dist().over(w_grp).alias("val_cume_dist"),
    )
)

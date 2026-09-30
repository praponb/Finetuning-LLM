from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy(F.col("val").asc_nulls_last())
w2 = Window.orderBy(F.col("val").desc())

flattened = df_price_tiers.select("id", F.explode_outer("tier_prices").alias("val"))

result = (
    flattened
    .select(
        "id",
        "val",
        F.cume_dist().over(w_running).alias("val_cume_dist"),
        F.rank().over(w2).alias("val_rank"),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("val")
w2 = Window.orderBy(F.col("val").desc_nulls_last())

exploded = df_price_tiers.select("id", F.explode_outer("discounts").alias("val"))

result = (
    exploded
    .select(
        "*",
        F.rank().over(w_hist).alias("val_rank"),
        F.percent_rank().over(w2).alias("val_percent_rank"),
        F.cume_dist().over(w_hist).alias("val_cume_dist"),
    )
)

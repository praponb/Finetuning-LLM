from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val").desc_nulls_first())
w_hist = Window.orderBy(F.col("val").asc_nulls_first())

lines = df_price_tiers.select("id", F.explode_outer("tier_limits").alias("val"))

result = (
    lines
    .withColumn("val_cume_dist", F.cume_dist().over(w))
    .withColumn("val_rank", F.rank().over(w_hist))
    .withColumn("val_dense_rank", F.dense_rank().over(w_hist))
)

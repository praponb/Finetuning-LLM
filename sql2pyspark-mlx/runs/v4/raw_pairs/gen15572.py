from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy("val")
w2 = Window.partitionBy("currency")
w3 = Window.partitionBy("currency").orderBy(F.col("val").desc_nulls_first())
w4 = Window.orderBy(F.col("currency_avg_val").desc_nulls_first())
w5 = Window.partitionBy("currency").orderBy(F.col("val_cume_dist").desc())

exploded = df_price_tiers.select("id", "currency", F.explode_outer("tier_limits").alias("val"))

with_windows = (
    exploded
    .select(
        "*",
        F.dense_rank().over(w).alias("val_dense_rank"),
        F.avg("val").over(w2).alias("currency_avg_val"),
        F.cume_dist().over(w3).alias("val_cume_dist"),
    )
)

valid = (
    with_windows
    .filter(F.col("val_dense_rank") != 6)
    .withColumn("currency_avg_val_per_val_cume_dist", (F.col("currency_avg_val") / F.nullif("val_cume_dist", F.lit(0))))
)

result = (
    valid
    .withColumn("currency_avg_val_dense_rank", F.dense_rank().over(w4))
    .withColumn("val_cume_dist_percent_rank", F.percent_rank().over(w5))
)

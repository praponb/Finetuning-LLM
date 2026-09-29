from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.partitionBy("currency").orderBy(F.col("val").desc_nulls_last())
w3 = Window.partitionBy("currency").orderBy(F.col("val").desc_nulls_first())

lines = (
    df_price_tiers
    .select("id", "effective_from", "currency", F.explode("discounts").alias("val"))
)

result = (
    lines
    .select(
        "id",
        "effective_from",
        F.stddev_samp("val").over(w).alias("currency_stddev_samp_val"),
        F.cume_dist().over(w2).alias("val_cume_dist"),
        F.dense_rank().over(w3).alias("val_dense_rank"),
        F.count("val").over(w).alias("currency_count_val"),
    )
)

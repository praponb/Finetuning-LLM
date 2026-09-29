from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("plan").orderBy(F.col("val").desc_nulls_last())
w2 = Window.orderBy("val")
w3 = Window.partitionBy("plan")

exploded = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("discounts").alias("val"))
)

result = (
    exploded
    .select(
        "*",
        F.dense_rank().over(w_recent).alias("val_dense_rank"),
        F.percent_rank().over(w2).alias("val_percent_rank"),
        F.stddev_samp("val").over(w3).alias("plan_stddev_samp_val"),
    )
)

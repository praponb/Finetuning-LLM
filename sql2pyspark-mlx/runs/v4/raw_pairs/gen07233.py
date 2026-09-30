from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("plan")
w_recent = Window.orderBy(F.col("val").desc_nulls_first())
w3 = Window.orderBy(F.col("val").desc_nulls_last())

flattened = df_price_tiers.select("id", "plan", F.explode_outer("tier_prices").alias("val"))

result = (
    flattened
    .select(
        "*",
        F.sum("val").over(w_ordered).alias("plan_sum_val"),
        F.max("val").over(w_ordered).alias("plan_max_val"),
        F.rank().over(w_recent).alias("val_rank"),
        F.dense_rank().over(w3).alias("val_dense_rank"),
    )
)

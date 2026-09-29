from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val").desc())
w2 = Window.partitionBy("plan").orderBy(F.col("val").asc_nulls_last())
w3 = Window.partitionBy("plan")
w4 = Window.orderBy(F.col("val").desc_nulls_last())

exploded = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("tier_prices").alias("val"))
)

result = (
    exploded
    .withColumn("val_dense_rank", F.dense_rank().over(w))
    .withColumn("val_dense_rank_2", F.dense_rank().over(w2))
    .withColumn("plan_stddev_samp_val", F.stddev_samp("val").over(w3))
    .withColumn("val_dense_rank_3", F.dense_rank().over(w4))
    .withColumn("plan_sum_val", F.sum("val").over(w3))
)

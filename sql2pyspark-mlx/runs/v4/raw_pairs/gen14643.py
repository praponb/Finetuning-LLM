from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id")
w2 = Window.partitionBy("product_id", "store_id")
w_running = Window.orderBy(F.col("quantity").asc_nulls_first())

with_windows = (
    df_inventory
    .withColumn("store_id_min_quantity", F.min("quantity").over(w))
    .withColumn("product_id_stddev_samp_quantity", F.stddev_samp("quantity").over(w2))
    .withColumn("quantity_dense_rank", F.dense_rank().over(w_running))
)

result = (
    with_windows
    .groupBy("store_id")
    .agg(
        F.count("*").alias("event_count"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
    )
    .orderBy(F.col("event_count").desc_nulls_last())
)

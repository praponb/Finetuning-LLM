from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_shipping_fee").desc())

result = (
    df_orders
    .groupBy("channel")
    .agg(F.sum("shipping_fee").alias("total_shipping_fee"))
    .withColumn("rnk", F.dense_rank().over(w))
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("shipping_fee")

result = (
    df_orders
    .withColumn("rnk", F.dense_rank().over(w))
    .filter(F.col("rnk") <= 3)
    .select("id", "store_id", "shipping_fee")
    .orderBy(F.col("store_id").desc(), F.col("id").desc())
)

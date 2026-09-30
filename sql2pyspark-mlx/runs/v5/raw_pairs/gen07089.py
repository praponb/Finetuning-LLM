from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_discount").desc())

result = (
    df_orders
    .groupBy("store_id")
    .agg(F.sum("discount").alias("total_discount"))
    .withColumn("discount_rank", F.rank().over(w))
)

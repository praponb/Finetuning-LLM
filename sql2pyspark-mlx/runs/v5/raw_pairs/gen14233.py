from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("channel")

result = (
    df_orders
    .groupBy("channel")
    .agg(F.sum("discount").alias("total_discount"))
    .withColumn("cumulative_discount", F.sum("total_discount").over(w))
    .orderBy("cumulative_discount")
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("customer_id")

result = (
    df_orders
    .groupBy("customer_id")
    .agg(F.sum("discount").alias("total_discount"))
    .withColumn("cumulative_discount", F.sum("total_discount").over(w))
    .orderBy("total_discount", "customer_id")
)

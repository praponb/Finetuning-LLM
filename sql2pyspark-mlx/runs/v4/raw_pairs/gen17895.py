from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("order_id")

result = (
    df_order_items
    .filter(F.col("product_id").isin([24, 36, 38]))
    .groupBy("order_id")
    .agg(F.sum("unit_price").alias("total_unit_price"))
    .withColumn("cumulative_unit_price", F.sum("total_unit_price").over(w))
)

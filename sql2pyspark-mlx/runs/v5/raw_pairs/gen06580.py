from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("product_id")

result = (
    df_order_items
    .filter(F.col("order_id").isin([13, 24]))
    .groupBy("product_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_quantity", F.sum("total_quantity").over(w))
)

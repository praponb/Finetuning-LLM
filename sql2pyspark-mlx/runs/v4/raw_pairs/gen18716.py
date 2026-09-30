from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("supplier_id")

result = (
    df_products
    .groupBy("supplier_id")
    .agg(F.sum("stock_qty").alias("total_stock_qty"))
    .withColumn("cumulative_stock_qty", F.sum("total_stock_qty").over(w))
)

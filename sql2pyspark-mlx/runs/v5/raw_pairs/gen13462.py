from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("category")

result = (
    df_products
    .filter(F.size("tags") > 2)
    .groupBy("category")
    .agg(
        F.sum("stock_qty").alias("total_stock_qty"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_stock_qty", F.sum("total_stock_qty").over(w))
)

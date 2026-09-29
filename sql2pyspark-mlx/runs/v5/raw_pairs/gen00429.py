from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("stock_qty_bucket")

result = (
    df_products
    .filter(F.col("price") < 350)
    .groupBy(
        F.when(F.col("stock_qty") >= 317, "high").when(F.col("stock_qty") >= 193, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(F.sum("stock_qty").alias("total_stock_qty"))
    .withColumn("cumulative_stock_qty", F.sum("total_stock_qty").over(w))
    .orderBy(F.col("cumulative_stock_qty").desc())
)

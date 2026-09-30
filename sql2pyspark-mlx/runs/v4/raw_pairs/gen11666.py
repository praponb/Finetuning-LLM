from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category")

result = (
    df_products
    .withColumn("max_stock_qty_by_category", F.max("stock_qty").over(w))
    .filter(F.col("stock_qty") == F.col("max_stock_qty_by_category"))
    .select("id", "category", "stock_qty", "price")
)

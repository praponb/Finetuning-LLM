from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_products
    .filter(F.col("stock_qty") < 340)
    .groupBy("category")
    .agg(F.sum("stock_qty").alias("total_stock_qty"))
    .withColumn("pct_of_total", F.round(((F.col("total_stock_qty") * 100) / F.sum("total_stock_qty").over(w)), 2))
    .orderBy("pct_of_total", "category")
    .limit(20)
)

from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.quarter("launch_date").alias("launch_quarter"), "category")
    .agg(F.min("stock_qty").alias("min_stock_qty"))
    .orderBy(F.col("launch_quarter").desc(), "category")
    .limit(3)
)

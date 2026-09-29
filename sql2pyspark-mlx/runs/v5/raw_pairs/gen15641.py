from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category").isin(["Books", "Sports", "Electronics"]))
    .groupBy(F.date_format("launch_date", "yyyy-MM").alias("launch_month"))
    .agg(F.min("stock_qty").alias("min_stock_qty"))
    .orderBy(F.col("launch_month").desc())
    .limit(50)
)

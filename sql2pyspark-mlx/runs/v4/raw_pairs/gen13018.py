from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand", "category")
    .agg(F.avg("stock_qty").alias("avg_stock_qty"))
    .filter(F.col("avg_stock_qty") < 250)
    .orderBy("category", "brand")
    .limit(3)
)

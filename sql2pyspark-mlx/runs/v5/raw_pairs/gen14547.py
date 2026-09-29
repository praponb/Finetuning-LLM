from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category", "supplier_id")
    .agg(F.avg("stock_qty").alias("avg_stock_qty"))
    .orderBy(F.col("supplier_id").desc(), "category")
    .limit(10)
)

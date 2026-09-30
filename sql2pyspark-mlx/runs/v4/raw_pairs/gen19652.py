from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"))
    .filter(F.col("avg_stock_qty") > 24)
    .orderBy(F.col("brand").desc())
)

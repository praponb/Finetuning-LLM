from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.month("launch_date").isin([1, 7, 10]))
    .groupBy("brand")
    .agg(F.round(F.stddev("stock_qty"), 2).alias("stddev_stock_qty"))
    .filter(F.col("stddev_stock_qty") < 27)
    .orderBy("brand", F.col("stddev_stock_qty").desc())
    .limit(1)
)

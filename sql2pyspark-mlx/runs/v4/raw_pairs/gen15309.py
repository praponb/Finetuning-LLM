from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(F.round(F.stddev("stock_qty"), 2).alias("stddev_stock_qty"))
)

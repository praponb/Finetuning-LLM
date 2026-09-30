from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([3, 4]) & F.month("sale_date").isin([4, 7, 11]))
    .agg(F.round(F.stddev("revenue"), 2).alias("stddev_revenue"))
)

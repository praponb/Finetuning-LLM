from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("revenue") >= 2500) & F.col("product_id").isin([3, 4, 5, 7]))
    .agg((F.max("revenue") - F.min("revenue")).alias("revenue_range"))
)

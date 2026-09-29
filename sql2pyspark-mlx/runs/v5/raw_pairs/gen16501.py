from pyspark.sql import functions as F

filtered_store_sales = (
    df_store_sales
    .filter(F.col("sale_date").between("2023-07-01", "2025-01-01") & F.col("product_id").isin([3, 9, 10]) & F.col("store_id").isin([1, 2, 3]))
)

store_sales_stats = (
    filtered_store_sales
    .groupBy("store_id")
    .agg(F.round(F.stddev("revenue"), 2).alias("stddev_revenue"))
)

result = store_sales_stats.select("store_id", "stddev_revenue")

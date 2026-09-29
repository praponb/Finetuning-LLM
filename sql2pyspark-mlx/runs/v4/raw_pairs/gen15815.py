from pyspark.sql import functions as F

filtered_store_sales = (
    df_store_sales
    .filter(F.col("sale_date").between("2024-05-01", "2024-08-01") & (F.col("units") <= 34) & (F.col("units") > 13))
    .withColumn("revenue_rounded", F.round("revenue", 1))
)

store_sales_stats = (
    filtered_store_sales
    .groupBy("product_id", F.quarter("sale_date").alias("sale_quarter"))
    .agg(F.round(F.stddev("revenue"), 2).alias("stddev_revenue"))
)

result = (
    store_sales_stats
    .select("product_id", "sale_quarter", "stddev_revenue")
    .orderBy("product_id")
)

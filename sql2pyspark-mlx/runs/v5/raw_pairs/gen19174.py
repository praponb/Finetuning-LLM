from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2023-12-01", "2024-09-01"))
    .groupBy("product_id", F.quarter("sale_date").alias("sale_quarter"))
    .agg(
        F.min("revenue").alias("min_revenue"),
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
    )
)

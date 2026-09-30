from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2023-03-09", "2024-04-01") & (F.col("revenue") > 3800))
    .groupBy(F.month("sale_date").alias("sale_month"))
    .agg(
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
        F.min("sale_date").alias("min_sale_date"),
        F.count("revenue").alias("revenue_count"),
    )
)

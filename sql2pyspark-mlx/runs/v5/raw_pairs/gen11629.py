from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").between(30, 32) & (F.col("sale_date") < "2024-09-23"))
    .groupBy(F.month("sale_date").alias("sale_month"))
    .agg(
        F.min("sale_date").alias("first_sale_date"),
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
        F.count("revenue").alias("revenue_count"),
    )
)

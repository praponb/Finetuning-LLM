from pyspark.sql import functions as F

filtered_store_sales = df_store_sales.filter(F.col("sale_date").between("2023-08-01", "2024-10-08"))

revenue_bucket_summary = (
    filtered_store_sales
    .groupBy(
        F.when(F.col("revenue") >= 3400, "high").when(F.col("revenue") >= 700, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.min("sale_date").alias("first_sale_date"),
    )
)

result = revenue_bucket_summary.filter(F.col("avg_revenue") < 77)

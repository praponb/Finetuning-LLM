from pyspark.sql import functions as F

base = (
    df_store_sales
    .filter((F.col("sale_date") < "2024-02-01") & (F.col("sale_date") >= "2025-02-19"))
)

store_sales_stats = (
    base
    .groupBy(
        F.when(F.col("revenue") >= 3000, "high").when(F.col("revenue") >= 900, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(F.max("sale_date").alias("last_sale_date"))
)

result = store_sales_stats

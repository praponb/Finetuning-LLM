from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        "product_id",
        F.quarter("sale_date").alias("sale_quarter"),
        F.when(F.col("revenue") >= 3800, "high").when(F.col("revenue") >= 1600, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(
        F.count("revenue").alias("revenue_count"),
        F.sum("revenue").alias("total_revenue"),
    )
)

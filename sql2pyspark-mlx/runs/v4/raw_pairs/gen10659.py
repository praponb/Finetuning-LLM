from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") >= 39)
    .groupBy(
        F.when(F.col("revenue") >= 4100, "high").when(F.col("revenue") >= 1800, "mid").otherwise("low").alias("revenue_bucket"),
        "store_id",
        "product_id",
    )
    .agg(
        F.max("revenue").alias("max_revenue"),
        F.count("*").alias("store_sale_count"),
        F.count("*").alias("row_count"),
    )
)

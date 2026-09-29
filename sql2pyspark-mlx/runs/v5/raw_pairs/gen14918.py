from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.when(F.col("revenue") >= 4400, "high").when(F.col("revenue") >= 800, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(
        F.min("sale_date").alias("min_sale_date"),
        F.count("revenue").alias("revenue_count"),
    )
)

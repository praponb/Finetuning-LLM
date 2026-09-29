from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.when(F.col("revenue") >= 3000, "high").when(F.col("revenue") >= 1800, "mid").otherwise("low").alias("revenue_bucket"),
        "product_id",
    )
    .agg(F.sum("revenue").alias("total_revenue"))
)

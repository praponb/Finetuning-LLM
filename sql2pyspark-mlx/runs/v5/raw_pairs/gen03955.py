from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") <= 12) & (F.col("revenue") >= 3400))
    .groupBy(
        F.when(F.col("revenue") >= 3800, "high").when(F.col("revenue") >= 1600, "mid").otherwise("low").alias("revenue_bucket"),
        "product_id",
    )
    .agg(F.count("revenue").alias("revenue_count"))
    .orderBy("product_id")
)

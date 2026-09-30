from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.when(F.col("revenue") >= 4200, "high").when(F.col("revenue") >= 1100, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg((F.max("revenue") - F.min("revenue")).alias("revenue_range"))
    .orderBy(F.col("revenue_bucket").desc())
)

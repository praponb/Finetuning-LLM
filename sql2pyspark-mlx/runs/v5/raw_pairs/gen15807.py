from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.when(F.col("units") >= 45, "high").when(F.col("units") >= 7, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg((F.max("revenue") - F.min("revenue")).alias("revenue_range"))
    .filter(F.col("revenue_range") >= 62)
    .orderBy("units_bucket", "revenue_range")
)

from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").between(1200, 3600))
    .groupBy(
        F.when(F.col("units") >= 33, "high").when(F.col("units") >= 19, "mid").otherwise("low").alias("units_bucket"),
        "store_id",
        F.when(F.col("revenue") >= 4400, "high").when(F.col("revenue") >= 1100, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg((F.max("units") - F.min("units")).alias("units_range"))
)

from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").between(20, 31))
    .groupBy(
        F.when(F.col("units") >= 36, "high").when(F.col("units") >= 14, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(F.max("revenue").alias("max_revenue"))
    .filter(F.col("max_revenue") >= 2100)
)

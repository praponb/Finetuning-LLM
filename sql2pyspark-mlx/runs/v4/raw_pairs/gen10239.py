from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.when(F.col("units") >= 40, "high").when(F.col("units") >= 6, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(F.min("revenue").alias("min_revenue"))
    .filter(F.col("min_revenue") < 2400)
)

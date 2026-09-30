from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.when(F.col("revenue") >= 4200, "high").when(F.col("revenue") >= 600, "mid").otherwise("low").alias("revenue_bucket"),
        F.when(F.col("units") >= 33, "high").when(F.col("units") >= 19, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
        F.round(F.stddev("units"), 2).alias("stddev_units"),
    )
    .orderBy("units_bucket")
)

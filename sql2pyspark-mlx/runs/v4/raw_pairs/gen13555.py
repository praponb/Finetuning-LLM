from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") == 24)
    .groupBy(
        F.when(F.col("units") >= 32, "high").when(F.col("units") >= 10, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        F.min("units").alias("min_units"),
        (F.max("units") - F.min("units")).alias("units_range"),
    )
    .orderBy("units_range", "units_bucket")
    .limit(5)
)

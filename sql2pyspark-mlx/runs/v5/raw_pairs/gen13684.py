from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 5]) | (F.col("units") == 29))
    .groupBy(
        F.when(F.col("units") >= 41, "high").when(F.col("units") >= 14, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        (F.max("units") - F.min("units")).alias("units_range"),
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
    )
    .filter(F.col("units_range") < 48)
    .orderBy("units_range", "units_bucket")
    .limit(1)
)

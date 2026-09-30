from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        "store_id",
        F.when(F.col("units") >= 44, "high").when(F.col("units") >= 20, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        F.avg("revenue").alias("avg_revenue"),
        F.round(F.stddev("units"), 2).alias("stddev_units"),
        F.count("revenue").alias("revenue_count"),
    )
    .orderBy(F.col("revenue_count").desc(), F.col("store_id").desc())
)

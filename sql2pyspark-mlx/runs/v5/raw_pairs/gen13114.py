from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        "store_id",
        F.when(F.col("units") >= 36, "high").when(F.col("units") >= 17, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        F.avg("revenue").alias("avg_revenue"),
        F.sum("revenue").alias("total_revenue"),
        F.min("units").alias("min_units"),
    )
    .orderBy(F.col("min_units").desc())
)

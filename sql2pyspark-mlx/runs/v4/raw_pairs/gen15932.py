from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.month("sale_date").isin([3, 5, 8]))
    .groupBy(
        F.when(F.col("units") >= 41, "high").when(F.col("units") >= 10, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        F.count("units").alias("units_count"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
    )
    .orderBy("units_bucket")
)

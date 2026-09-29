from pyspark.sql import functions as F

valid = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 4]))
    .withColumn("units_rounded", F.round("units", 0))
)

result = (
    valid
    .groupBy("product_id")
    .agg(
        F.percentile_approx("revenue", [0.25, 0.5, 0.75]).alias("revenue_quartiles"),
        F.avg("revenue").alias("avg_revenue"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue_2"),
    )
    .filter(F.col("avg_revenue") >= 0)
    .orderBy(F.col("avg_revenue_2").desc_nulls_last())
)

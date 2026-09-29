from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 2, 5, 8]))
    .agg(
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
        F.min("units").alias("min_units"),
    )
    .orderBy(F.col("stddev_revenue").desc())
)

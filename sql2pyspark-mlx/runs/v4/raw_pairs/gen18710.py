from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("revenue") > 3400) & F.col("store_id").isin([1, 2, 3, 4]))
    .groupBy("product_id", "store_id")
    .agg(
        F.max("revenue").alias("max_revenue"),
        F.min("units").alias("min_units"),
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
    )
    .orderBy(F.col("min_units").desc())
)

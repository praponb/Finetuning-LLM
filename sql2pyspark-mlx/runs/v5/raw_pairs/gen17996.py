from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 6]))
    .groupBy("product_id")
    .agg(
        F.min("units").alias("min_units"),
        F.round(F.stddev("units"), 2).alias("stddev_units"),
    )
)

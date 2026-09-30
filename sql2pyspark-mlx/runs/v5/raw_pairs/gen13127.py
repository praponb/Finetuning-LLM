from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([4, 5]) | F.col("product_id").isin([1, 5, 8, 9]))
    .groupBy("product_id", "store_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.sum("quantity").alias("total_quantity"),
    )
    .orderBy(F.col("total_quantity").desc())
)

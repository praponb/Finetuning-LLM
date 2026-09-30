from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id") == 1)
    .groupBy("product_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)

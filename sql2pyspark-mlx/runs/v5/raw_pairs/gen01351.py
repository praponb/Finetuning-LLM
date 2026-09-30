from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id") == 2)
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.max("quantity").alias("max_quantity"),
    )
)

from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.countDistinct("quantity").alias("unique_products"),
    )
    .filter(F.col("unique_products") >= 2)
)

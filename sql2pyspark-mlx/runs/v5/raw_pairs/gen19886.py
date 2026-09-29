from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("store_id") == 2) & (F.col("store_id").isin([2, 3, 4, 6]) | (F.col("quantity") == 114)))
    .groupBy("product_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.countDistinct("quantity").alias("unique_products"),
    )
)

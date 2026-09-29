from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id", "store_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.max("last_restock").alias("last_last_restock"),
    )
    .orderBy("product_id")
)

from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id") == 9)
    .groupBy("store_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.max("quantity").alias("max_quantity"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
    .orderBy(F.col("max_quantity").desc(), "avg_quantity")
)

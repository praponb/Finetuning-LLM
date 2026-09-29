from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("product_id") == 1) & F.col("product_id").isin([1, 5]))
    .groupBy("product_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.avg("quantity").alias("avg_quantity"),
    )
    .orderBy("avg_quantity", F.col("stddev_quantity").desc(), "product_id")
    .limit(10)
)

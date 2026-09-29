from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id", "store_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.count("*").alias("num_inventory"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.avg("quantity").alias("avg_quantity"),
    )
    .filter(F.col("avg_quantity") > 160)
    .drop("avg_quantity")
    .orderBy(F.col("store_id").desc())
)

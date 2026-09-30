from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 4, 5]))
    .groupBy(
        F.when(F.col("quantity") >= 211, "high").when(F.col("quantity") >= 88, "mid").otherwise("low").alias("quantity_bucket"),
        "store_id",
    )
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.avg("quantity").alias("avg_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
    .orderBy(F.col("quantity_count").desc())
)

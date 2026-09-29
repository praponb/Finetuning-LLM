from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([3, 5]) & (F.col("store_id").isin([1, 2, 3, 4]) | (F.col("quantity") > 215)))
    .groupBy(
        "store_id",
        "product_id",
        F.when(F.col("quantity") >= 182, "high").when(F.col("quantity") >= 116, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
    .orderBy(F.col("product_id").desc())
)

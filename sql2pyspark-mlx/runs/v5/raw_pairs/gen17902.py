from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.when(F.col("quantity") >= 245, "high").when(F.col("quantity") >= 102, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
    .orderBy(F.col("avg_quantity").desc(), "stddev_quantity", "quantity_bucket")
    .limit(10)
)

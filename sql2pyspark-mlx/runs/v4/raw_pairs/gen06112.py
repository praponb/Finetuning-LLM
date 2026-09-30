from pyspark.sql import functions as F

result = (
    df_inventory
    .rollup(
        F.when(F.col("quantity") >= 222, "high").when(F.col("quantity") >= 60, "mid").otherwise("low").alias("quantity_bucket"),
        "product_id",
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
    .orderBy("product_id")
)

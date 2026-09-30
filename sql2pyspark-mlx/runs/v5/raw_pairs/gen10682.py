from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.min("quantity").alias("min_quantity"),
    )
    .filter(F.col("min_quantity") < 180)
    .drop("min_quantity")
    .orderBy(F.col("store_id").desc(), "avg_quantity")
    .limit(3)
)

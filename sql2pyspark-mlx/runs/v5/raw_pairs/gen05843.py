from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([5, 7]))
    .groupBy("store_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.min("quantity").alias("min_quantity"),
    )
    .filter(F.col("avg_quantity") < 37)
    .select("store_id", "avg_quantity", "min_quantity")
    .orderBy(F.col("min_quantity").desc())
)

from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock") < "2024-06-29") & F.col("store_id").isin([1, 3, 5, 6]))
    .groupBy("store_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.max("quantity").alias("max_quantity"),
    )
    .filter(F.col("max_quantity") > 60)
)

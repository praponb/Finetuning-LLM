from pyspark.sql import functions as F

store_summary = (
    df_inventory
    .filter(F.col("product_id").isin([1, 5, 6]))
    .groupBy("store_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(
        F.col("s.sqft"),
        F.col("s.store_name"),
        F.col("ss.avg_quantity"),
        F.col("ss.stddev_quantity"),
    )
)

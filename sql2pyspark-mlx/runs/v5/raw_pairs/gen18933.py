from pyspark.sql import functions as F

store_summary = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.sum("quantity").alias("total_quantity"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("S"), F.col("s.id") == F.col("S.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.store_name"),
        F.col("s.city"),
        F.col("S.min_quantity"),
        F.col("S.total_quantity"),
    )
    .orderBy("city")
)

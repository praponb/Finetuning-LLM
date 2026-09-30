from pyspark.sql import functions as F

store_totals = (
    df_inventory
    .groupBy("store_id")
    .agg(F.count("quantity").alias("quantity_count"))
)

result = (
    df_stores.alias("stores")
    .join(store_totals.alias("store_totals"), F.col("stores.id") == F.col("store_totals.store_id"), "inner")
    .filter(F.col("store_totals.quantity_count") >= 9)
    .select(
        F.col("stores.id"),
        F.col("stores.region"),
        F.col("stores.city"),
        F.col("store_totals.quantity_count"),
    )
    .orderBy(F.col("region").desc(), F.col("city").desc())
)

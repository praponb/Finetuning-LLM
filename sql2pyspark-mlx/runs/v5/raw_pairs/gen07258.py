from pyspark.sql import functions as F

store_totals = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
)

result = (
    df_stores.alias("stores")
    .join(store_totals.alias("store_totals"), F.col("stores.id") == F.col("store_totals.store_id"), "inner")
    .filter(F.col("store_totals.avg_quantity") >= 46)
    .select(
        F.col("stores.store_name"),
        F.col("stores.sqft"),
        F.col("store_totals.max_quantity"),
        F.col("store_totals.avg_quantity"),
    )
    .orderBy(F.col("max_quantity").desc())
)

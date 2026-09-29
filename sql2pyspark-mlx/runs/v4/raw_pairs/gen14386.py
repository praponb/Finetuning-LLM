from pyspark.sql import functions as F

inventory_by_store = (
    df_inventory
    .filter(F.col("quantity") == 135)
    .groupBy("store_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.max("last_restock").alias("latest_last_restock"),
    )
)

result = (
    df_stores.alias("s")
    .join(inventory_by_store.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .filter(F.col("t.max_quantity") >= 200)
    .select(
        F.col("s.sqft"),
        F.col("s.city"),
        F.col("t.max_quantity"),
        F.col("t.latest_last_restock"),
    )
)

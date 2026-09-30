from pyspark.sql import functions as F

inventory_by_store = (
    df_inventory
    .filter(F.col("store_id").isin([1, 6]))
    .groupBy("store_id")
    .agg(F.min("last_restock").alias("first_last_restock"))
)

result = (
    df_stores.alias("s")
    .join(inventory_by_store.alias("t"), F.col("s.id") == F.col("t.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.opened_date"),
        F.col("s.sqft"),
        F.col("t.first_last_restock"),
    )
    .orderBy("opened_date")
)

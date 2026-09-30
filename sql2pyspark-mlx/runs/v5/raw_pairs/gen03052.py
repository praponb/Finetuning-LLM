from pyspark.sql import functions as F

inventory_by_store = (
    df_inventory
    .filter(F.col("quantity").between(150, 196))
    .groupBy("store_id")
    .agg(F.min("quantity").alias("min_quantity"))
)

result = (
    df_stores.alias("stores")
    .join(inventory_by_store.alias("inventory_by_store"), F.col("stores.id") == F.col("inventory_by_store.store_id"), "inner")
    .filter(F.col("inventory_by_store.min_quantity") > 220)
    .select(
        F.col("stores.city"),
        F.col("stores.sqft"),
        F.col("inventory_by_store.min_quantity"),
    )
)

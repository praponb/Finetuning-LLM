from pyspark.sql import functions as F

inventory_by_store = (
    df_inventory
    .filter(F.col("quantity") == 151)
    .groupBy("store_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.countDistinct("quantity").alias("unique_stores"),
    )
)

result = (
    df_stores.alias("s")
    .join(inventory_by_store.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.opened_date"),
        F.col("s.sqft"),
        F.col("t.min_quantity"),
        F.col("t.unique_stores"),
    )
    .orderBy(F.col("sqft").desc(), "unique_stores", "id")
    .limit(5)
)

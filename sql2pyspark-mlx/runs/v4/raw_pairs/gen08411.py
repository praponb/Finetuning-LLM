from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter((F.col("i.product_id") == 2) | F.col("s.sqft").between(7702, 11755))
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
    )
    .orderBy("store_name", "last_restock")
)

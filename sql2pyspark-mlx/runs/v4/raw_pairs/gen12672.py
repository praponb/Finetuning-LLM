from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .filter(F.col("s.city") != "Singapore")
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("i.last_restock"),
        F.col("i.product_id"),
        F.col("s.sqft"),
        F.col("s.city"),
        F.col("s.opened_date"),
    )
    .orderBy(F.col("city").desc())
)

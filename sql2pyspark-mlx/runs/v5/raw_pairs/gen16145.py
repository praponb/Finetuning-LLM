from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter((F.col("i.quantity") > 149) & (F.col("s.sqft") > 6468))
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("i.product_id"),
        F.col("s.store_name"),
        F.col("s.city"),
        F.col("s.opened_date"),
    )
    .orderBy("city")
)

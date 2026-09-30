from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("i.quantity"),
        F.col("s.store_name"),
        F.col("p.brand"),
        F.col("p.cost"),
        F.date_add(F.col("i.last_restock"), 3).alias("last_restock_plus_3d"),
    )
)

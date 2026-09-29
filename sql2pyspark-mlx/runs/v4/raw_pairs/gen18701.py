from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter((F.col("i.product_id") == 8) & F.month(F.col("i.last_restock")).isin([3, 4, 11]))
    .select(
        F.col("i.quantity"),
        F.col("s.opened_date"),
        F.col("s.city"),
        F.col("p.brand"),
    )
)

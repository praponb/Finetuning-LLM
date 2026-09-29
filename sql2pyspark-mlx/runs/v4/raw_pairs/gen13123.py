from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "right")
    .filter(F.col("p.price").isNotNull())
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("i.store_id"),
        F.col("s.store_name"),
        F.col("s.city"),
        F.col("p.category"),
        F.col("p.stock_qty"),
    )
)

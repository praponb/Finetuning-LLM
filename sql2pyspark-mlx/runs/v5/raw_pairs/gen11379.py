from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter((F.col("p.price") < 760) & F.col("s.opened_date").between("2024-11-25", "2025-01-01"))
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("i.product_id"),
        F.col("s.city"),
        F.col("p.supplier_id"),
        F.col("p.brand"),
    )
)

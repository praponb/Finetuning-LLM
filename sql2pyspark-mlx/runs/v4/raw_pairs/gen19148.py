from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .filter(F.col("s.sqft").between(5637, 12064) & (F.year(F.col("s.opened_date")) >= 2025))
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("i.product_id"),
        F.col("s.store_name"),
        F.col("s.city"),
        F.col("p.supplier_id"),
    )
)

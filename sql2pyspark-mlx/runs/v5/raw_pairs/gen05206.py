from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("i.last_restock") <= "2025-05-02")
    .select(
        F.col("i.quantity"),
        F.col("p.stock_qty"),
        F.col("p.brand"),
        F.col("s.rating"),
        F.col("s.country"),
    )
)

from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "right")
    .filter(F.col("oi.quantity").between(4, 5))
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("oi.order_id"),
        F.col("p.stock_qty"),
        F.col("p.product_name"),
        F.col("s.country"),
        F.trim(F.col("p.category")).alias("clean_category"),
    )
)

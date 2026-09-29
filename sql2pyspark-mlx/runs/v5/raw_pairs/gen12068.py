from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "right")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.rating").between(1.9, 2.3))
    .select(
        F.col("oi.id"),
        F.col("oi.order_id"),
        F.col("p.category"),
        F.col("p.product_name"),
        F.col("s.supplier_name"),
        F.col("s.rating"),
    )
)

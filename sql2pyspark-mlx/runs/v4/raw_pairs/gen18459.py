from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("p.brand") != "Initech")
    .groupBy(F.col("s.city"))
    .agg(
        F.max(F.col("oi.quantity")).alias("max_quantity"),
        F.count(F.col("oi.quantity")).alias("quantity_count"),
    )
)

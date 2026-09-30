from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .filter(F.col("p.supplier_id").isin([1, 4, 6]) & F.array_contains(F.col("p.tags"), "bestseller"))
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("p.supplier_id"),
        F.col("p.brand"),
        F.col("p.price"),
        (F.col("oi.quantity") - F.col("oi.unit_price")).alias("line_total"),
    )
)

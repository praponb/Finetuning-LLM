from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "right")
    .filter(~(F.col("s.country").isin(["TH", "UK", "JP"]) & F.col("p.product_name").like("Off%")))
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("p.product_name"),
        F.col("s.supplier_name"),
        F.col("s.rating"),
    )
    .orderBy(F.col("id").desc())
)

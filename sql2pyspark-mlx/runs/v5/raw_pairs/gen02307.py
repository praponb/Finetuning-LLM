from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("oi.order_id").isin([20, 26, 38, 40]) & F.col("oi.order_id").isin([7, 29, 37]))
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("oi.order_id"),
        F.col("p.brand"),
        F.col("p.product_name"),
        F.col("s.country"),
    )
)

from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "outer")
    .filter(F.col("oi.order_id").isin([5, 12, 20]) & ~F.col("s.city").isin(["Chiang Mai", "Singapore"]))
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("p.product_name"),
        F.col("s.supplier_name"),
        F.col("s.rating"),
    )
    .orderBy("supplier_name")
)

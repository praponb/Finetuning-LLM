from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .filter((F.col("p.supplier_id") == 28) & F.array_contains(F.col("p.tags"), "eco"))
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("o.store_id"),
        F.col("o.order_date"),
        F.col("p.brand"),
        F.col("p.stock_qty"),
    )
)

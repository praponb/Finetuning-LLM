from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .filter(F.month(F.col("p.launch_date")).isin([2, 6, 8]) | (F.col("oi.quantity") == 6))
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("oi.order_id"),
        F.col("p.supplier_id"),
        F.col("p.cost"),
        F.col("o.status"),
        F.col("o.customer_id"),
    )
)

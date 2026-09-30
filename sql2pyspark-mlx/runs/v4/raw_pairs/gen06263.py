from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .filter(~(F.col("oi.order_id").isin([5, 22]) & (F.col("o.order_date") <= "2025-01-01")))
    .select(
        F.col("oi.order_id"),
        F.col("o.shipping_fee"),
        F.col("o.channel"),
        F.col("p.supplier_id"),
    )
    .orderBy(F.col("supplier_id").desc())
)

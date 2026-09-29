from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.col("o.shipping_fee") > 30)
    .select(
        F.col("oi.product_id"),
        F.col("oi.quantity"),
        F.col("o.shipping_fee"),
        F.col("p.brand"),
        F.col("p.category"),
    )
    .orderBy("product_id", "shipping_fee")
)

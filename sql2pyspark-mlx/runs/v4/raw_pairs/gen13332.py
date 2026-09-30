from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter((F.col("oi.quantity") != 5) & (F.col("p.category") == "Fashion"))
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("p.product_name"),
        F.col("o.customer_id"),
        F.col("o.shipping_fee"),
    )
    .orderBy("product_id", "id")
    .limit(10)
)

from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "right")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "right")
    .select(
        F.col("oi.quantity"),
        F.col("oi.product_id"),
        F.col("p.cost"),
        F.col("o.shipping_fee"),
        F.weekofyear(F.col("o.order_date")).alias("order_date_weekofyear"),
    )
)

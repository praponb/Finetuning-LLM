from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "right")
    .filter((F.col("o.amount") < 570) & F.col("o.customer_id").isNotNull())
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("oi.order_id"),
        F.col("o.status"),
        F.col("o.discount"),
        F.col("p.price"),
    )
)

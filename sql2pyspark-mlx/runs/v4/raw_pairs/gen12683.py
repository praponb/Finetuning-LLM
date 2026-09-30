from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "right")
    .select(
        F.col("oi.quantity"),
        F.col("o.shipping_fee"),
        F.col("p.cost"),
        F.col("p.launch_date"),
    )
    .orderBy(F.col("shipping_fee").desc())
)

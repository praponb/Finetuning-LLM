from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "outer")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "outer")
    .select(
        F.col("oi.quantity"),
        F.col("oi.product_id"),
        F.col("o.order_date"),
        F.col("o.shipping_fee"),
        F.col("p.launch_date"),
        F.col("p.stock_qty"),
    )
    .orderBy("product_id")
)

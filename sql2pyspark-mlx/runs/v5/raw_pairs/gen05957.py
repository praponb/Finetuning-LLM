from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.customer_id") == 22)
    .select(
        F.col("oi.product_id"),
        F.col("oi.order_id"),
        F.col("p.stock_qty"),
        F.col("o.order_date"),
        F.col("o.store_id"),
    )
)

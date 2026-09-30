from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .select(
        F.col("oi.id"),
        F.col("oi.quantity"),
        F.col("oi.product_id"),
        F.col("p.stock_qty"),
        F.col("p.category"),
        F.col("o.customer_id"),
    )
    .orderBy(F.col("customer_id").desc(), F.col("stock_qty").desc())
)

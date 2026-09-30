from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "right")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "right")
    .filter(F.col("oi.product_id") == 22)
    .select(
        F.col("oi.id"),
        F.col("oi.quantity"),
        F.col("p.stock_qty"),
        F.col("p.cost"),
        F.col("o.channel"),
    )
    .orderBy(F.col("stock_qty").desc(), F.col("quantity").desc())
)

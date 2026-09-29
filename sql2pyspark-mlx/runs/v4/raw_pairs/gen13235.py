from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "right")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.channel").isNotNull())
    .select(
        F.col("oi.product_id"),
        F.col("p.stock_qty"),
        F.col("o.channel"),
        F.col("o.status"),
    )
    .orderBy(F.col("stock_qty").desc())
)

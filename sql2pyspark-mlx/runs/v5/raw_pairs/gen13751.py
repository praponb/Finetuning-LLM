from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .filter(F.col("oi.quantity") > 7)
    .select(
        F.col("oi.order_id"),
        F.col("p.launch_date"),
        F.col("p.cost"),
        F.col("o.channel"),
        F.col("o.shipping_fee"),
    )
    .orderBy(F.col("order_id").desc())
)

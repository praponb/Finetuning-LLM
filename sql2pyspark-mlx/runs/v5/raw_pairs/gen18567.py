from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "right")
    .filter(F.col("p.price").isNull())
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("p.cost"),
        F.col("o.channel"),
        F.col("o.shipping_fee"),
    )
    .orderBy("channel")
)

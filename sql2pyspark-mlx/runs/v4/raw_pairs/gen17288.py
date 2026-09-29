from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .filter(F.col("o.shipping_fee") > 15)
    .select(
        F.col("o.shipping_fee"),
        F.col("o.order_date"),
        F.col("o.amount"),
        F.col("oi.unit_price"),
    )
    .orderBy(F.col("order_date").desc())
)

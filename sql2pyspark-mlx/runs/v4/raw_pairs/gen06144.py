from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("order_items.order_id"), "inner")
    .filter(F.col("orders.amount").between(730, 1150))
    .select(
        F.col("order_items.quantity"),
        F.col("orders.shipping_fee"),
        F.col("orders.discount"),
    )
    .orderBy(F.col("discount").desc())
)

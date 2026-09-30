from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("order_items.order_id"), "left")
    .filter(F.col("order_items.order_id").isin([28, 33, 37]))
    .select(
        F.col("order_items.id"),
        F.col("order_items.product_id"),
        F.col("orders.shipping_fee"),
        F.col("orders.customer_id"),
    )
    .orderBy("product_id", "id")
    .limit(20)
)

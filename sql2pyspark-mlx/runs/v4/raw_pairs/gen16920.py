from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.filter(F.col("customer_id").isNull()).alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "left_semi")
    .filter(F.col("order_items.product_id") == 36)
    .select(
        F.col("order_items.id"),
        F.col("order_items.quantity"),
        F.col("order_items.order_id"),
    )
    .orderBy(F.col("quantity").desc())
)

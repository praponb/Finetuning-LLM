from pyspark.sql import functions as F

order_totals = (
    df_order_items
    .filter(F.col("order_id").isin([11, 35, 37]))
    .groupBy("order_id")
    .agg(
        F.countDistinct("unit_price").alias("unique_orders"),
        F.countDistinct("quantity").alias("unique_products"),
    )
)

result = (
    df_orders.alias("orders")
    .join(order_totals.alias("t"), F.col("orders.id") == F.col("t.order_id"), "left")
    .select(
        F.col("orders.id"),
        F.col("orders.order_date"),
        F.col("orders.store_id"),
        F.col("t.unique_orders"),
        F.col("t.unique_products"),
    )
)

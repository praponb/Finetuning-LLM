from pyspark.sql import functions as F

order_totals = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
    )
)

result = (
    df_orders.alias("orders")
    .join(order_totals.alias("order_totals"), F.col("orders.id") == F.col("order_totals.order_id"), "left")
    .select(
        F.col("orders.id"),
        F.col("orders.shipping_fee"),
        F.col("orders.customer_id"),
        F.col("order_totals.total_quantity"),
        F.col("order_totals.stddev_unit_price"),
    )
    .orderBy(F.col("id").desc())
)

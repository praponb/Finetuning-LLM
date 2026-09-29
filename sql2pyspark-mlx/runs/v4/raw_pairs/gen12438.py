from pyspark.sql import functions as F

order_items_by_order = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
        F.max("quantity").alias("max_quantity"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_items_by_order.alias("oibo"), F.col("o.id") == F.col("oibo.order_id"), "left")
    .filter(F.col("oibo.max_quantity") > 3.5)
    .select(
        F.col("o.order_date"),
        F.col("oibo.stddev_unit_price"),
        F.col("oibo.max_quantity"),
    )
    .orderBy(F.col("max_quantity").desc())
)

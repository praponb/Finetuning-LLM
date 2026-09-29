from pyspark.sql import functions as F

order_items_by_order = (
    df_order_items
    .filter(F.col("unit_price").between(210, 240))
    .groupBy("order_id")
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_items_by_order.alias("t"), F.col("o.id") == F.col("t.order_id"), "left")
    .filter(F.col("t.row_count") >= 5)
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("t.row_count"),
        F.col("t.avg_quantity"),
    )
    .orderBy(F.col("id").desc())
)

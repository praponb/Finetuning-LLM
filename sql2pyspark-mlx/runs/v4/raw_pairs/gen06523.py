from pyspark.sql import functions as F

order_items_by_order = (
    df_order_items
    .filter(F.col("quantity").between(4, 5))
    .groupBy("order_id")
    .agg(
        F.max("unit_price").alias("max_unit_price"),
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_items_by_order.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .filter(F.col("t.revenue") >= 42)
    .select(
        F.col("o.id"),
        F.col("o.store_id"),
        F.col("o.amount"),
        F.col("t.max_unit_price"),
        F.col("t.revenue"),
    )
    .orderBy("id")
)

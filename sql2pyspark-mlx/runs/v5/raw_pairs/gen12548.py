from pyspark.sql import functions as F

order_items_by_order = (
    df_order_items
    .filter(F.col("product_id").isin([8, 32]))
    .groupBy("order_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.avg("unit_price").alias("avg_unit_price"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_items_by_order.alias("O"), F.col("o.id") == F.col("O.order_id"), "left")
    .filter(F.col("O.avg_unit_price") >= 270)
    .select(
        F.col("o.id"),
        F.col("o.store_id"),
        F.col("O.quantity_count"),
        F.col("O.avg_unit_price"),
    )
    .orderBy(F.col("store_id").desc())
)

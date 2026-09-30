from pyspark.sql import functions as F

order_totals = (
    df_order_items
    .groupBy("order_id")
    .agg(F.count("quantity").alias("quantity_count"))
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "inner")
    .filter(F.col("ot.quantity_count") > 10)
    .select(F.col("o.store_id"), F.col("o.shipping_fee"), F.col("ot.quantity_count"))
)

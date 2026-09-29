from pyspark.sql import functions as F

order_summary = (
    df_order_items
    .groupBy("order_id")
    .agg(F.max("unit_price").alias("max_unit_price"))
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .filter(F.col("os.max_unit_price") > 270)
    .select(F.col("o.customer_id"), F.col("os.max_unit_price"))
    .orderBy(F.col("customer_id").desc())
)

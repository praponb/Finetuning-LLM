from pyspark.sql import functions as F

order_summary = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.sum("quantity").alias("total_quantity"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .filter(F.col("t.min_quantity") >= 7.5)
    .select(
        F.col("o.order_date"),
        F.col("o.shipping_fee"),
        F.col("t.min_quantity"),
        F.col("t.total_quantity"),
    )
    .orderBy(F.col("shipping_fee").desc())
)

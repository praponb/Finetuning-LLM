from pyspark.sql import functions as F

order_summary = (
    df_order_items
    .groupBy("order_id")
    .agg(F.min("unit_price").alias("min_unit_price"))
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("o.amount"),
        F.col("os.min_unit_price"),
    )
    .orderBy(F.col("id").desc())
)

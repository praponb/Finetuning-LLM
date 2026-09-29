from pyspark.sql import functions as F

order_summary = (
    df_order_items
    .filter(F.col("order_id") == 27)
    .groupBy("order_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.sum("unit_price").alias("total_unit_price"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("O"), F.col("o.id") == F.col("O.order_id"), "inner")
    .filter(F.col("O.total_unit_price") >= 1900)
    .select(
        F.col("o.id"),
        F.col("o.status"),
        F.col("o.store_id"),
        F.col("O.total_quantity"),
        F.col("O.total_unit_price"),
    )
    .orderBy(F.col("store_id").desc())
)

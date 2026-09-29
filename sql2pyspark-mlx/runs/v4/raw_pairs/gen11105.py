from pyspark.sql import functions as F

order_totals = (
    df_order_items
    .groupBy("order_id")
    .agg(F.countDistinct("unit_price").alias("unique_orders"))
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("o.discount"),
        F.col("ot.unique_orders"),
    )
    .orderBy(F.col("discount").desc())
)

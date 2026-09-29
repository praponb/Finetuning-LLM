from pyspark.sql import functions as F

order_totals = (
    df_order_items
    .groupBy("order_id")
    .agg(F.sum("unit_price").alias("total_unit_price"))
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "inner")
    .filter(F.col("ot.total_unit_price") > 2000)
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("o.customer_id"),
        F.col("ot.total_unit_price"),
    )
    .orderBy("discount", "id")
    .limit(5)
)

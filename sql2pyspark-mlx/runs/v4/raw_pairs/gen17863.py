from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.min("amount").alias("min_amount"),
        F.sum("amount").alias("total_amount"),
    )
)

result = (
    df_orders.alias("o")
    .join(payments_by_order.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .filter(F.col("t.total_amount") > 4300)
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("o.amount"),
        F.col("t.min_amount"),
        F.col("t.total_amount"),
    )
)

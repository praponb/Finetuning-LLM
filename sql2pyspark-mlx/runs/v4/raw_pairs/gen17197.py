from pyspark.sql import functions as F

order_totals = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.count("*").alias("cnt"),
        F.min("paid_at").alias("min_paid_at"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("O"), F.col("o.id") == F.col("O.order_id"), "inner")
    .filter(F.col("O.cnt") > 7)
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("o.customer_id"),
        F.col("O.cnt"),
        F.col("O.min_paid_at"),
    )
    .orderBy(F.col("customer_id").desc(), "id")
    .limit(10)
)

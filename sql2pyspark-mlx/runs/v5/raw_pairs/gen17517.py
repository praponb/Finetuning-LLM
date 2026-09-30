from pyspark.sql import functions as F

order_totals = (
    df_payments
    .groupBy("order_id")
    .agg(F.min("paid_at").alias("first_paid_at"))
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.order_date"),
        F.col("o.customer_id"),
        F.col("ot.first_paid_at"),
    )
    .orderBy(F.col("customer_id").desc(), "id")
    .limit(50)
)

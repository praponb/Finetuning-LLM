from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "inner")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .groupBy(F.col("customers.segment"))
    .agg(
        F.min(F.col("payments.paid_at")).alias("first_paid_at"),
        F.max(F.col("payments.amount")).alias("max_amount"),
    )
    .orderBy(F.col("segment").desc())
)

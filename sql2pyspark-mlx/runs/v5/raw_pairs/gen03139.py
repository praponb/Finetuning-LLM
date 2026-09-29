from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "outer")
    .groupBy(F.col("o.customer_id"))
    .agg(
        F.min(F.col("p.amount")).alias("min_amount"),
        F.min(F.col("p.paid_at")).alias("first_paid_at"),
    )
)

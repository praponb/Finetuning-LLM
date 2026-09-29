from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.col("p.paid_at") < "2023-06-02")
    .select(
        F.col("p.id"),
        F.col("p.paid_at"),
        F.col("p.status"),
        F.col("o.customer_id"),
        F.col("o.amount"),
        F.col("o.order_date"),
    )
)

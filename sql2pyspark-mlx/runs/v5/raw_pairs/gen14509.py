from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter((F.col("p.method") == "card") & (F.col("p.paid_at") < "2024-08-01"))
    .select(
        F.col("p.id"),
        F.col("p.status"),
        F.col("p.amount"),
        F.col("p.order_id"),
        F.col("o.store_id"),
        F.col("o.status").alias("order_status"),
        F.col("o.amount").alias("order_amount"),
    )
    .orderBy("order_amount")
)

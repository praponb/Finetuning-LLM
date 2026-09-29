from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "left")
    .filter(F.col("p.amount").isNotNull() & (F.col("o.customer_id") == 12))
    .select(
        F.col("p.amount"),
        F.col("p.status"),
        F.col("p.method"),
        F.col("o.discount"),
    )
)

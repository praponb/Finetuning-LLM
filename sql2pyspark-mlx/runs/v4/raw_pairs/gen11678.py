from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "left")
    .filter(~(F.col("p.order_id").isin([1, 18, 21]) & F.col("o.amount").isNotNull()))
    .select(
        F.col("o.customer_id"),
        F.col("o.order_date"),
        F.col("o.store_id"),
        F.col("p.status"),
        F.col("p.paid_at"),
        F.col("p.method"),
    )
    .orderBy("paid_at", "method")
)

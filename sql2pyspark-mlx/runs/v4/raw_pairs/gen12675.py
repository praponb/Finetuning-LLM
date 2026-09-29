from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "outer")
    .filter(F.col("o.shipping_fee").between(23, 27) & F.col("o.customer_id").isin([10, 15]))
    .select(
        F.col("p.id"),
        F.col("p.amount"),
        F.col("p.paid_at"),
        F.col("o.status"),
        F.col("o.channel"),
        F.col("o.store_id"),
    )
)

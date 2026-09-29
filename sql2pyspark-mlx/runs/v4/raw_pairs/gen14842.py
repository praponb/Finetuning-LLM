from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("p.method").isin(["card", "promptpay", "bank_transfer"]))
    .select(
        F.col("p.id"),
        F.col("p.amount"),
        F.col("p.status"),
        F.col("o.discount"),
        F.col("c.city"),
        F.col("c.email"),
    )
)

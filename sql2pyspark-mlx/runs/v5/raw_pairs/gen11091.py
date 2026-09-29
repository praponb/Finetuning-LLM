from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.col("p.status").isin(["FAILED", "SUCCESS", "REFUNDED"]))
    .select(
        F.col("p.paid_at"),
        F.col("o.customer_id"),
        F.col("o.discount"),
        F.col("c.credit_limit"),
    )
)

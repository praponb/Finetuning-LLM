from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.credit_limit") < 14600)
    .select(
        F.col("p.id"),
        F.col("p.order_id"),
        F.col("o.customer_id"),
        F.col("c.email"),
        F.concat(F.col("p.method"), F.lit("/"), F.col("p.status")).alias("method_status"),
    )
)

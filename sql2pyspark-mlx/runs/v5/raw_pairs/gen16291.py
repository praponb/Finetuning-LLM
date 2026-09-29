from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .select(
        F.col("p.order_id"),
        F.col("p.method"),
        F.col("o.order_date"),
        F.col("c.segment"),
        F.col("c.is_active"),
    )
)

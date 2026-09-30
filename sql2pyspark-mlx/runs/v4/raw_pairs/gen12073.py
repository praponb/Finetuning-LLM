from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "outer")
    .select(
        F.col("p.id"),
        F.col("p.status"),
        F.col("o.customer_id"),
        F.col("o.store_id"),
        F.col("c.city"),
        F.col("c.name"),
    )
    .orderBy(F.col("customer_id").desc(), "status")
)

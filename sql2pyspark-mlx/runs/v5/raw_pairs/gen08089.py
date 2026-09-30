from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("p.status") == "SUCCESS")
    .select(
        F.col("p.id"),
        F.col("p.status"),
        F.col("p.method"),
        F.col("o.discount"),
        F.col("o.status").alias("order_status"),
        F.col("c.name"),
    )
    .orderBy(F.col("method").desc())
)

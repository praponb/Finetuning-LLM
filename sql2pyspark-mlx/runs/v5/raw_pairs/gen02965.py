from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.col("o.store_id").isNotNull())
    .select(
        F.col("p.method"),
        F.col("p.amount"),
        F.col("o.amount").alias("order_amount"),
        F.col("c.is_active"),
        F.upper(F.col("p.status")).alias("status_upper"),
    )
)

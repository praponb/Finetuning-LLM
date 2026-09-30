from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.customer_id").isNotNull())
    .select(
        F.col("p.id"),
        F.col("p.method"),
        F.col("p.paid_at"),
        F.col("o.store_id"),
        F.col("o.customer_id"),
        F.col("o.status"),
        F.when(F.col("p.method").isin(["promptpay", "cash"]), "Group A").otherwise("Other").alias("method_group"),
    )
)

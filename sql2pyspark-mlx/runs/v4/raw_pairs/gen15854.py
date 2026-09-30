from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.store_id").isNotNull())
    .select(
        F.col("p.amount"),
        F.col("p.paid_at"),
        F.col("o.shipping_fee"),
        F.col("c.age"),
        F.col("c.email"),
        F.when(F.col("p.method").isin(["card", "promptpay"]), "Tier 1").otherwise("Other").alias("method_group"),
    )
)

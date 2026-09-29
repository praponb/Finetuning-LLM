from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.amount"),
        F.col("p.method"),
        F.col("p.paid_at"),
        F.col("o.customer_id"),
        F.when(F.col("o.channel") == "partner", 1).otherwise(0).alias("is_partner"),
    )
)

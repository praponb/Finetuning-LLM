from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "inner")
    .filter((F.col("o.shipping_fee") <= 39) & F.col("o.channel").isin(["partner", "mobile"]))
    .select(
        F.col("o.shipping_fee"),
        F.col("o.status"),
        F.col("o.customer_id"),
        F.col("p.method"),
        F.col("p.paid_at"),
    )
    .orderBy(F.col("shipping_fee").desc(), F.col("customer_id").desc())
)

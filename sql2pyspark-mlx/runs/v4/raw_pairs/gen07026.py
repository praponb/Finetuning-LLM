from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "left")
    .filter((F.col("o.shipping_fee") >= 29) & F.col("p.order_id").isNull())
    .select(
        F.col("o.store_id"),
        F.col("o.customer_id"),
        F.col("p.status"),
        F.col("p.method"),
        F.col("p.paid_at"),
        F.least(F.col("o.amount"), F.col("o.shipping_fee")).alias("least_amount_shipping_fee"),
    )
)

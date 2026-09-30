from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "inner")
    .filter(F.col("p.amount").between(990, 1450) & (F.col("o.status") == "COMPLETED"))
    .select(
        F.col("o.discount"),
        F.col("o.shipping_fee"),
        F.col("o.amount"),
        F.col("p.order_id"),
        F.col("p.status"),
        F.col("p.paid_at"),
    )
)

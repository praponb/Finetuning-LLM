from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "left")
    .filter(~F.col("p.method").isin(["bank_transfer", "card", "promptpay"]))
    .select(
        F.col("p.id"),
        F.col("p.order_id"),
        F.col("p.paid_at"),
        F.col("o.status"),
        F.col("s.region"),
        F.col("s.city"),
    )
)

from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter((F.col("p.method") == "card") | F.col("p.method").isin(["promptpay", "bank_transfer"]))
    .select(
        F.col("o.shipping_fee"),
        F.col("o.amount"),
        F.col("p.paid_at"),
        F.col("p.status"),
        F.col("p.method"),
    )
    .orderBy(F.col("amount").desc())
)

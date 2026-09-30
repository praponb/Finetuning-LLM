from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter((F.col("p.status") == "SUCCESS") & (F.col("p.method") != "bank_transfer"))
    .select(
        F.col("p.order_id"),
        F.col("p.method"),
        F.col("o.status"),
        F.col("o.shipping_fee"),
        F.date_trunc("YEAR", F.col("p.paid_at")).alias("paid_at_period"),
    )
    .orderBy("order_id")
)

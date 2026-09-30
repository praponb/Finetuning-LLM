from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("p.amount") > 700)
    .select(
        F.col("p.method"),
        F.col("p.paid_at"),
        F.col("o.shipping_fee"),
        F.col("o.store_id"),
    )
    .orderBy(F.col("store_id").desc(), "shipping_fee")
)

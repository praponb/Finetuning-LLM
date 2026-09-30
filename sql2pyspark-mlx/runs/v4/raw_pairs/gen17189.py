from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(~((F.to_date(F.col("p.paid_at")) < "2023-07-01") & F.col("p.status").isin(["REFUNDED", "FAILED", "SUCCESS"])))
    .select(
        F.col("p.id"),
        F.col("p.order_id"),
        F.col("p.amount"),
        F.col("p.status"),
        F.col("o.shipping_fee"),
        F.col("o.order_date"),
        F.col("o.status").alias("order_status"),
        F.least(F.col("o.shipping_fee"), F.col("o.discount")).alias("least_shipping_fee_discount"),
    )
)

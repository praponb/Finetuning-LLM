from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.store_name").like("%Kios%"))
    .select(
        F.col("p.id"),
        F.col("p.paid_at"),
        F.col("p.order_id"),
        F.col("o.shipping_fee"),
        F.col("s.region"),
        F.round((F.col("o.discount") / F.col("o.amount")), 2).alias("discount_per_amount"),
    )
)

from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("o.discount") <= 0.21)
    .select(
        F.col("p.paid_at"),
        F.col("o.customer_id"),
        F.col("o.store_id"),
        F.col("s.store_name"),
    )
    .orderBy(F.col("store_id").desc())
)

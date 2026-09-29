from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "inner")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "left")
    .filter(F.col("orders.discount") <= 0.11)
    .select(
        F.col("payments.id"),
        F.col("payments.paid_at"),
        F.col("payments.method"),
        F.col("orders.channel"),
        F.col("orders.status"),
        F.col("stores.opened_date"),
    )
)

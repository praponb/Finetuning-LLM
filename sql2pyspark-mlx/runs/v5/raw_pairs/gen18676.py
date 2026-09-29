from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "inner")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .select(
        F.col("payments.id"),
        F.col("payments.method"),
        F.col("orders.status"),
        F.col("orders.shipping_fee"),
        F.col("stores.region"),
        F.col("stores.store_name"),
    )
)

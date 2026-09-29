from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "inner")
    .filter((F.col("orders.shipping_fee") <= 37) | (F.col("payments.amount") >= 550))
    .select(
        F.col("payments.order_id"),
        F.col("orders.discount"),
        F.col("orders.store_id"),
        F.col("orders.customer_id"),
    )
)

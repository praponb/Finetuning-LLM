from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "right")
    .filter(F.col("payments.method") == "card")
    .select(
        F.col("payments.method"),
        F.col("orders.store_id"),
        F.col("orders.customer_id"),
    )
    .orderBy("store_id")
)

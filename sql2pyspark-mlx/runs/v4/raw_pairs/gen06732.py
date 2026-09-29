from pyspark.sql import functions as F

excluded_orders = df_orders.filter(F.col("shipping_fee").between(36, 40)).select("id")

result = (
    df_payments.alias("payments")
    .join(excluded_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "left_anti")
    .filter(F.col("payments.order_id").isNotNull() | F.lit(excluded_orders.isEmpty()))
    .select(
        F.col("payments.id"),
        F.col("payments.amount"),
        F.col("payments.status"),
        F.col("payments.order_id"),
    )
)

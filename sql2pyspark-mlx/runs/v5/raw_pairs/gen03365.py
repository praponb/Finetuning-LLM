from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_payments.filter(F.col("amount").between(1100, 1290)).alias("payments"), F.col("orders.id") == F.col("payments.order_id"), "left_anti")
    .select(F.col("orders.id"), F.col("orders.shipping_fee"), F.col("orders.status"))
)

from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "left")
    .filter((F.col("orders.shipping_fee") < 38) & (F.col("payments.amount") > 1020))
    .select(F.col("payments.amount"), F.col("orders.order_date"))
    .orderBy("order_date")
)

from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_payments.filter(F.col("status") == "SUCCESS").alias("payments"), F.col("orders.id") == F.col("payments.order_id"), "left_semi")
    .select(F.col("orders.id"), F.col("orders.order_date"))
)

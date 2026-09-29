from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.filter(F.col("status") == "CANCELLED").alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "left_semi")
    .filter(F.col("payments.order_id").isin([9, 38]))
    .select(F.col("payments.id"), F.col("payments.paid_at"), F.col("payments.status"))
    .orderBy("status", "id")
)

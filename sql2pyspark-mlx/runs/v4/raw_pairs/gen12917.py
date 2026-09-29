from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.filter(F.col("paid_at") >= "2025-03-01").alias("p"), F.col("o.id") == F.col("p.order_id"), "left_anti")
    .filter(F.col("o.customer_id") == 28)
    .select(F.col("o.id"), F.col("o.order_date"))
)

from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.filter(F.col("paid_at") >= "2023-05-20").alias("p"), F.col("o.id") == F.col("p.order_id"), "left_anti")
    .select(F.col("o.id"), F.col("o.amount"))
)

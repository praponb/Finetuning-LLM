from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.filter(F.col("status") == "CANCELLED").alias("o"), F.col("p.order_id") == F.col("o.id"), "left_semi")
    .filter(F.col("p.paid_at") >= "2023-11-01")
    .select(F.col("p.id"), F.col("p.amount"), F.col("p.status"))
)

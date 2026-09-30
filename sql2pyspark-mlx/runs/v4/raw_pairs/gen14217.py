from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.filter(F.col("status") != "SHIPPED").alias("o"), F.col("p.order_id") == F.col("o.id"), "left_semi")
    .filter(F.to_date(F.col("p.paid_at")) < "2024-02-16")
    .select(F.col("p.id"), F.col("p.paid_at"))
    .orderBy("paid_at")
)

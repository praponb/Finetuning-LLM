from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.filter(F.col("amount").between(1010, 1220)).alias("o"), F.col("p.order_id") == F.col("o.id"), "left_semi")
    .select(F.col("p.id"), F.col("p.order_id"), F.col("p.paid_at"))
    .orderBy("order_id", "paid_at")
)

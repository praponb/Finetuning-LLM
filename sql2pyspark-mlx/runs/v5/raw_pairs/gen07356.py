from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.filter(F.col("customer_id") == 13).alias("o"), F.col("p.order_id") == F.col("o.id"), "left_semi")
    .select(F.col("p.id"), F.col("p.method"), F.col("p.paid_at"))
    .orderBy(F.col("method").desc())
)

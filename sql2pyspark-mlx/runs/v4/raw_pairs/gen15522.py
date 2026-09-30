from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "right")
    .filter((F.col("p.amount") >= 1280) & F.col("p.method").isNotNull())
    .select(F.col("p.id"), F.col("p.status"), F.col("p.paid_at"), F.col("o.channel"))
)

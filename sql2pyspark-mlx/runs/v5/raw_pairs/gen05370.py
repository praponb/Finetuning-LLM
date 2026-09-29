from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .groupBy(F.col("o.store_id"))
    .agg(F.max(F.col("p.paid_at")).alias("last_paid_at"))
    .orderBy(F.col("store_id").desc(), "last_paid_at")
    .limit(1)
)

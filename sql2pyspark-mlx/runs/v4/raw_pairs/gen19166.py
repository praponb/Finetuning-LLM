from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "outer")
    .filter(F.hour(F.col("p.paid_at")).between(10, 17))
    .groupBy(F.col("o.status"), F.col("o.channel"))
    .agg(F.round(F.avg(F.col("p.amount")), 2).alias("avg_amount"))
    .filter(F.col("avg_amount") >= 77)
    .orderBy(F.col("status").desc())
)

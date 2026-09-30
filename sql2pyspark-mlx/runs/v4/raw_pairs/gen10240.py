from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.customer_id").isin([4, 18]))
    .groupBy(F.col("o.channel"), F.col("o.status"))
    .agg(F.round(F.avg(F.col("p.amount")), 2).alias("avg_amount"))
)
